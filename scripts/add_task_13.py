import urllib.request
import json
import configparser
import pathlib

cfg = pathlib.Path.home() / ".databrickscfg"
p = configparser.ConfigParser()
p.read(str(cfg))
token = p.get("DEFAULT", "token", fallback="")
host = "https://dbc-3d6ea2d4-8d3f.cloud.databricks.com"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

job_id = "32552891389630"

# 1. Fetch current job settings
req = urllib.request.Request(f"{host}/api/2.1/jobs/get?job_id={job_id}", headers=headers)
with urllib.request.urlopen(req) as r:
    job_data = json.loads(r.read())

settings = job_data["settings"]
tasks = settings["tasks"]

# Check if task already exists
task_key = "gold_sync_operational_feedback"
existing = [t for t in tasks if t["task_key"] == task_key]

new_task = {
    "task_key": task_key,
    "depends_on": [
        {"task_key": "gold_scored_transactions"},
        {"task_key": "gold_merchant_risk_monitoring"},
    ],
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
        "notebook_path": "/Workspace/Users/fahimmeethale@gmail.com/fraud-and-transaction-risk-monitoring/notebooks/gold/sync_operational_feedback",
        "source": "WORKSPACE",
    },
    "timeout_seconds": 0,
    "environment_key": "Default",
    "disabled": False,
}

if existing:
    print(f"Task '{task_key}' already exists in job {job_id}. Updating definition...")
    for idx, t in enumerate(tasks):
        if t["task_key"] == task_key:
            tasks[idx] = new_task
else:
    print(f"Adding task '{task_key}' as Task #13 to job {job_id}...")
    tasks.append(new_task)

payload = {
    "job_id": int(job_id),
    "new_settings": {
        "tasks": tasks,
    },
}

update_req = urllib.request.Request(
    f"{host}/api/2.1/jobs/update",
    data=json.dumps(payload).encode("utf-8"),
    headers=headers,
)
with urllib.request.urlopen(update_req) as r:
    res = r.read()

print(f"SUCCESS: Job {job_id} now has {len(tasks)} tasks.")
