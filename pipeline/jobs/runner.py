# pipeline/jobs/runner.py
import sys
import subprocess
import os

JOB_MAP = {
    "schema_mismatch": "pipeline/jobs/schema_mismatch_job.py",
    "oom": "pipeline/jobs/oom_job.py",
    "skew": "pipeline/jobs/skew_job.py",
}

def run(job_key):
    path = JOB_MAP.get(job_key)
    if not path:
        print("Unknown job", job_key)
        sys.exit(2)

    # Run the job as a subprocess so we get a non-zero exit on failure (like spark-submit)
    print(f"Running job: {job_key} -> {path}")
    completed = subprocess.run([sys.executable, path], capture_output=False)
    sys.exit(completed.returncode)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: runner.py <schema_mismatch|oom|skew>")
        sys.exit(2)
    run(sys.argv[1])
