
"""
Run predefined Commvault schedule policies 'now' using cvpysdk.

- Authenticates to Command Center
- Supports targeting by policy NAME or by taskId (schedule policy ID)
- Calls policy.run_now() and prints started job IDs
- Optional: polls Job Controller to list jobs started in the recent window

Requires:
    pip install cvpysdk

Docs:
    Developer portal: https://developer.commvault.com/python
    PyPI: https://pypi.org/project/cvpysdk/
"""

from datetime import datetime, timedelta, timezone
from cvpysdk.commcell import Commcell
from cvpysdk.policies.schedule_policies import SchedulePolicies

# -------------------------------
# CONFIGURATION — EDIT THESE
# -------------------------------
HOST = "https://<CommandCenterHostName>"    # e.g., https://commserve.domain.com
USER = "<username>"
PASS = "<password>"

# Choose one of the two input lists below:

# A) Target by schedule policy NAMES (recommended for readability)
POLICY_NAMES = [
    # "Nightly FS Incrementals",
    # "Weekly Synth Full Copy",
    # "DB Backups - Prod"
]

# B) Target by schedule policy task IDs (if you already have the IDs)
POLICY_TASK_IDS = [
    # 451,
    # 777
]

# If your SDK returns only a single jobId for run_now() but the policy starts many jobs,
# set this to True to also query recent jobs started by you in a short time window.
QUERY_RECENT_JOBS = True

# Time window (minutes) to look back for jobs you just started
RECENT_WINDOW_MINUTES = 3


def run_by_names(commcell: Commcell, names: list[str]) -> None:
    """Run schedule policies by NAME and print job IDs."""
    sp = SchedulePolicies(commcell)

    # Build map: name -> object
    all_policies = sp.all_schedule_policies()  # dict of {name: id}
    print(f"[INFO] Found {len(all_policies)} schedule policies on CommCell.")

    for name in names:
        if not sp.has_policy(name):
            print(f"[WARN] Schedule policy not found: '{name}'")
            continue

        policy = sp.get(name)
        try:
            # Triggers the policy immediately (equivalent to 'Run now' in UI)
            job_ids = policy.run_now()
            print(f"[OK] '{name}' started. Job IDs from SDK: {job_ids}")
        except Exception as e:
            print(f"[ERROR] Run now failed for '{name}': {e}")


def run_by_task_ids(commcell: Commcell, ids: list[int]) -> None:
    """Run schedule policies by taskId and print job IDs."""
    sp = SchedulePolicies(commcell)

    for task_id in ids:
        try:
            # get() accepts name or id on recent SDK builds
            policy = sp.get(task_id)
        except Exception:
            print(f"[WARN] Schedule policy with taskId={task_id} not found.")
            continue

        try:
            job_ids = policy.run_now()
            print(f"[OK] taskId={task_id} started. Job IDs from SDK: {job_ids}")
        except Exception as e:
            print(f"[ERROR] Run now failed for taskId={task_id}: {e}")


def list_recent_jobs(commcell: Commcell) -> None:
    """
    Workaround: if policy.run_now() returns only the first jobId while the policy
    actually kicked off multiple jobs, list recent jobs started in the last N minutes.
    """
    jc = commcell.job_controller

    # Filter the job list to a short time window and the current user
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=RECENT_WINDOW_MINUTES)
    all_jobs = jc.all_jobs()  # returns a dict keyed by jobId with details

    # Example fields in entries vary by version; we defensively parse submitTime/userName if present.
    recent = []
    for jid, details in (all_jobs or {}).items():
        try:
            # submitTime may be epoch seconds or ISO string depending on build; handle common case
            submit_time = details.get("submitTime") or details.get("startTime")
            # Normalize times where possible (if present)
            if isinstance(submit_time, (int, float)):
                submitted = datetime.fromtimestamp(submit_time, tz=timezone.utc)
            else:
                submitted = None
            user = (details.get("userName") or details.get("user") or "").lower()

            if submitted and submitted >= cutoff and USER.lower() in user:
                recent.append((jid, submitted, details.get("status")))
        except Exception:
            # Skip any malformed entries
            continue

    if recent:
        recent.sort(key=lambda x: x[1])  # sort by submitted time
        print(f"[INFO] Jobs submitted by '{USER}' in last {RECENT_WINDOW_MINUTES} min:")
        for jid, t, status in recent:
            print(f"   - JobId={jid} at {t.isoformat()} status={status}")
    else:
        print(f"[INFO] No recent jobs found for '{USER}' in the last {RECENT_WINDOW_MINUTES} minutes.")


def main():
    print("[INFO] Connecting to CommCell ...")
    with Commcell(HOST, USER, PASS) as commcell:
        # Run by names
        if POLICY_NAMES:
            run_by_names(commcell, POLICY_NAMES)

        # Run by IDs
        if POLICY_TASK_IDS:
            run_by_task_ids(commcell, POLICY_TASK_IDS)

        # Optional: list recent jobs as a workaround for multi-job policies
        if QUERY_RECENT_JOBS:
            list_recent_jobs(commcell)

    print("[INFO] Completed.")


if __name__ == "__main__":
    main()
