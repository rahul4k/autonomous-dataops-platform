# pipeline/jobs/runner.py
import sys
import subprocess
import os
from datetime import datetime

JOB_MAP = {
    "schema_mismatch": "pipeline/jobs/schema_mismatch_job.py",
    "oom": "pipeline/jobs/oom_job.py",
    "skew": "pipeline/jobs/skew_job.py",
}

LOG_DIR = "logs"   # save all logs here

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def run(job_key):
    job_path = JOB_MAP.get(job_key)
    if not job_path:
        print("Unknown job", job_key)
        sys.exit(2)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create folder: logs/<job_name>/
    job_log_dir = os.path.join(LOG_DIR, job_key)
    ensure_dir(job_log_dir)

    # log file path
    log_file = os.path.join(job_log_dir, f"run_{timestamp}.log")

    print(f"[Runner] Running job: {job_key}")
    print(f"[Runner] Log file: {log_file}")

    # run the job and capture stdout/stderr
    with open(log_file, "w") as f:
        proc = subprocess.Popen(
            [sys.executable, job_path],
            stdout=f,
            stderr=subprocess.STDOUT,
        )
        proc.wait()

    # Exit accordingly
    sys.exit(proc.returncode)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: runner.py <schema_mismatch|oom|skew>")
        sys.exit(2)
    run(sys.argv[1])