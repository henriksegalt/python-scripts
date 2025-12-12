
#!/usr/bin/env python3
"""
Execute restores for *restore schedules* by taskId:
- Reads each schedule's properties (associations + restoreOptions)
- Submits an immediate restore via CreateTask (Restore)
- Prints jobIds

Refs:
- Login token: https://docs.commvault.com/v11/software/rest_api_authentication_post_login_01.html
- Get schedule properties: http://api.commvault.com/docs/SP40/api/cv/ScheduleandSchedulePolicyOperations/get-schedules-properties/
- Create Task (Restore) (example payload; adapt per workload): https://docs.commvault.com/2023e/software/rest_api_post_create_task_restore.html
"""

import base64
import json
import sys
import time
from typing import Any, Dict, List, Optional

import requests

# -------------------------------
# CONFIG — EDIT THESE
# -------------------------------
# Command Center / WebConsole base (NO trailing slash)
BASE = "https://<CommandCenterHostName>/commandcenter/api"

USER = "<username>"
PASS = "<password>"  # will be base64-UTF8 encoded per REST Login doc

# List of restore schedule taskIds you want to execute now
RESTORE_SCHEDULE_TASK_IDS = [
    # 1234,
    # 5678,
]

# Optional overrides: used when schedule is missing required fields
OVERRIDES = {
    # Example: out-of-place destination for File System restores
    # Provide at least destClient and destPath for out-of-place restores
    "destination": {
        "destClient": {"clientName": "target-client"},
        "destPath": ["E:\\restore_target"]
    },
    # If you want to force overwrite/ACL behavior
    "overwriteFiles": True,
    "restoreACLsType": "ACL_DATA"  # ACL_ONLY | DATA_ONLY | ACL_DATA
    # Add other restoreOptions fields you need (varies by agent/workload)
}

# Set to False only for quick testing with self-signed certs; prefer True in production
VERIFY_TLS = True

# -------------------------------
# HELPERS
# -------------------------------

def login() -> str:
    """Returns Authtoken for subsequent calls."""
    payload = {
        "username": USER,
        "password": base64.b64encode(PASS.encode("utf-8")).decode("ascii"),
        "timeout": 30
    }
    r = requests.post(
        f"{BASE}/Login",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        data=json.dumps(payload),
        verify=VERIFY_TLS
    )
    r.raise_for_status()
    token = r.json().get("token")
    if not token:
        raise RuntimeError("Login succeeded but no token in response.")
    return token


def get_schedule_properties(token: str, task_id: int) -> Dict[str, Any]:
    """GET /Schedules/{taskId} returns schedule properties (associations, subTasks...)."""
    r = requests.get(
        f"{BASE}/Schedules/{task_id}",
        headers={"Accept": "application/json", "Authtoken": token},
        verify=VERIFY_TLS
    )
    r.raise_for_status()
    return r.json()


def build_restore_payload(props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build CreateTask payload from schedule properties.
    Applies OVERRIDES where needed.

    Notes:
    - For File System restore, include destination and flags in restoreOptions.
    - For other workloads (VM/DB), adjust fields to your agent's schema.
    """
    ti = props.get("taskInfo", {})
    associations = ti.get("associations", [])
    sub_tasks = ti.get("subTasks", [])

    if not associations:
        raise RuntimeError("Schedule has no associations; cannot build restore request.")

    # Find first RESTORE subtask/options
    restore_opts: Dict[str, Any] = {}
    for st in sub_tasks:
        subtask = st.get("subTask", {})
        if (subtask.get("operationType") == "RESTORE" or
            subtask.get("subTaskType") == "RESTORE"):
            restore_opts = st.get("options", {}).get("restoreOptions", {})
            break

    # Apply overrides on top of schedule's restoreOptions
    merged_restore_opts = dict(restore_opts)
    # Apply destination override if schedule lacks it
    if OVERRIDES.get("destination"):
        merged_restore_opts["destination"] = OVERRIDES["destination"]
    # Apply common flags (examples)
    for k in ("overwriteFiles", "restoreACLsType"):
        if OVERRIDES.get(k) is not None:
            merged_restore_opts[k] = OVERRIDES[k]

    # Sanity checks
    dest = merged_restore_opts.get("destination", {})
    if not dest and not is_in_place_restore(merged_restore_opts):
        raise RuntimeError(
            "Destination missing and schedule does not appear to be in-place. "
            "Provide OVERRIDES['destination'] with destClient and destPath."
        )

    payload = {
        "taskInfo": {
            "task": {
                "initiatedFrom": "COMMANDLINE",     # immediate run
                "taskType": 0,                      # IMMEDIATE
                "policyType": "DATA_PROTECTION",
                "taskFlags": {"disabled": False}
            },
            "associations": normalize_associations(associations),
            "subTasks": [
                {
                    "subTask": {"subTaskType": "RESTORE", "operationType": "RESTORE"},
                    "options": {"restoreOptions": merged_restore_opts}
                }
            ]
        }
    }
    return payload


def normalize_associations(associations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Reduce association objects to the minimal fields required by CreateTask (Restore).
    Common fields: clientName/appName/instanceId/backupsetId/subclientId...
    """
    out = []
    for a in associations:
        out.append({
            "clientName": a.get("clientName"),
            "appName": a.get("appName"),
            "instanceId": a.get("instanceId"),
            "backupsetId": a.get("backupsetId"),
            "subclientId": a.get("subclientId"),
            # Add more if your workload expects them
        })
    return out


def is_in_place_restore(restore_opts: Dict[str, Any]) -> bool:
    """
    Heuristic: treat as in-place if destination block is absent OR maps to same source client/path.
    Adjust if you use explicit 'inPlace' flags in your schedules.
    """
    dest = restore_opts.get("destination")
    return not dest  # crude heuristic; refine as needed for your environment


def submit_restore(token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /CreateTask to start the restore; returns response with jobIds."""
    r = requests.post(
        f"{BASE}/CreateTask",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authtoken": token
        },
        data=json.dumps(payload),
        verify=VERIFY_TLS
    )
    r.raise_for_status()
    return r.json()


def logout(token: str) -> None:
    try:
        requests.post(
            f"{BASE}/Logout",
            headers={"Accept": "application/json", "Authtoken": token},
            verify=VERIFY_TLS
        )
    except Exception:
        pass


def main():
    if not RESTORE_SCHEDULE_TASK_IDS:
        print("[ERROR] No schedule taskIds configured. Edit RESTORE_SCHEDULE_TASK_IDS.")
        sys.exit(1)

    print("[INFO] Logging in...")
    token = login()

    try:
        for task_id in RESTORE_SCHEDULE_TASK_IDS:
            print(f"[INFO] Reading schedule properties for taskId={task_id} ...")
            props = get_schedule_properties(token, task_id)

            payload = build_restore_payload(props)

            print(f"[INFO] Submitting restore for taskId={task_id} ...")
            resp = submit_restore(token, payload)
            job_ids = resp.get("jobIds") or resp.get("jobIds", [])
            print(f"[OK] taskId={task_id} restore submitted. Job IDs: {job_ids}")

    finally:
        logout(token)
        print("[INFO] Logged out.")

if __name__ == "__main__":
    main()
