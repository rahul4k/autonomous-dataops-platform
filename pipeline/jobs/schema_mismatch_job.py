# pipeline/jobs/schema_mismatch_job.py
import json
import sys

def run():
    # Simulated schema expectation
    expected_columns = {"user_id", "name", "created_at", "amount"}

    # Simulated incoming data with a missing column 'amount' and extra 'amt'
    incoming = [
        {"user_id": 1, "name": "Alice", "created_at": "2025-01-01T00:00:00", "amt": 100},
        {"user_id": 2, "name": "Bob", "created_at": "2025-01-02T00:00:00", "amt": 200},
    ]

    # Discover incoming columns
    colset = set()
    for row in incoming:
        colset.update(row.keys())

    missing = expected_columns - colset
    extra = colset - expected_columns

    if missing:
        # Raise an error similar to what would happen in pipeline when strict schema is used
        msg = f"SchemaMismatchError: missing columns: {sorted(list(missing))}; extra columns: {sorted(list(extra))}"
        raise RuntimeError(msg)

    print("Schema OK")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("Job failed:", e)
        # Exit non-zero to indicate Airflow failure / job failure
        sys.exit(1)
