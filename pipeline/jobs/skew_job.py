# pipeline/jobs/skew_job.py
import sys
import random
import time
from collections import Counter

def run():
    # Generate synthetic left table with highly skewed key distribution
    n = 100000
    keys = ["hotkey"] * 95000 + [f"key_{i}" for i in range(5000)]
    left = [{"id": i, "sk": random.choice(keys)} for i in range(n)]

    # right table small dimension
    right = [{"sk": "hotkey", "val": 1}] + [{"sk": f"key_{i}", "val": 2} for i in range(100)]

    # Simulate join by counting frequency of keys in left and right
    left_counts = Counter(x["sk"] for x in left)
    right_counts = Counter(x["sk"] for x in right)
    # Produce a synthetic "shuffle_bytes" estimation
    total_records = sum(left_counts.values())
    hotkey_count = left_counts["hotkey"]
    skew_ratio = hotkey_count / total_records

    print(f"INFO: total_records={total_records}, hotkey_count={hotkey_count}, skew_ratio={skew_ratio:.3f}")
    print("INFO: Estimated shuffle_bytes ~", int(total_records * 200))  # arbitrary metric

    # If skew ratio > 0.5, raise a simulated performance alert (but not a hard failure)
    if skew_ratio > 0.5:
        # Agent should detect skew and recommend salting or repartition
        msg = f"PerformanceAlert: extreme key skew detected on column sk, skew_ratio={skew_ratio:.3f}"
        print("WARN:", msg)
        # Exit non-zero to indicate the pipeline experienced an issue (for demo)
        raise RuntimeError(msg)

    print("Join completed")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("Job failed:", e)
        sys.exit(1)
