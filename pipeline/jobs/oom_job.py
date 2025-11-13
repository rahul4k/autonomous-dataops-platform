# pipeline/jobs/oom_job.py
import sys
import time

def run():
    # Demo: create a deliberate large allocation sentinel and then raise OOM-like error
    print("Starting heavy operation to simulate OOM...")
    time.sleep(1)
    # Instead of actually exhausting memory, we mimic the failure log
    oom_message = "java.lang.OutOfMemoryError: Java heap space - simulated by demo"
    # Print to stdout as Spark would log this
    print("ERROR:", oom_message)
    # Raise non-zero exit
    raise RuntimeError(oom_message)

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("Job failed:", e)
        sys.exit(1)
