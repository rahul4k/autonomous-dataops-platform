# tools/simulate_event.py
"""
Create a simple event.json for your local runs.

Usage:
  python tools/simulate_event.py --task schema_mismatch --job-file pipeline/jobs/schema_mismatch_job.py

This writes `event.json` in current folder which you can pass to orchestrator:
  python tools/orchestrator.py --event event.json
"""
import argparse
import json
import os
from datetime import datetime

def write_event(task: str, job_file: str, log_path: str = None):
    now = datetime.utcnow().isoformat() + "Z"
    if not log_path:
        # take latest log in logs/<task>/
        folder = os.path.join("logs", task)
        if os.path.exists(folder):
            files = sorted([os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".log")])
            log_path = files[-1] if files else None

    event = {
        "source": "local-run",
        "dag_id": "adp_sample_failures",
        "task_id": task,
        "run_id": f"manual__{now}",
        "log_path": log_path,
        "job_file": job_file,
        "metadata": {"env": "local"}
    }
    with open("event.json", "w") as fh:
        json.dump(event, fh, indent=2)
    print("Wrote event.json:", os.path.abspath("event.json"))
    return event

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--job-file", required=True)
    parser.add_argument("--log-path", default=None)
    args = parser.parse_args()
    write_event(args.task, args.job_file, args.log_path)
