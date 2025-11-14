# tests/test_fixer_optimizer.py
import os
import glob
import unittest
import json
from agents.fixer_optimizer import generate_patch_for_job, load_rules

# Helper: find a sample job file (schema_mismatch job) and create a sample root_cause
SAMPLE_JOB = "pipeline/jobs/schema_mismatch_job.py"
SAMPLE_OOM_JOB = "pipeline/jobs/oom_job.py"
SAMPLE_SKEW_JOB = "pipeline/jobs/skew_job.py"

class TestFixerOptimizer(unittest.TestCase):
    def setUp(self):
        self.rules = load_rules()

    def test_no_file(self):
        with self.assertRaises(FileNotFoundError):
            generate_patch_for_job("nonexistent.py", {"suggested_action_types": ["code_patch"]}, self.rules)

    def test_schema_rename_patch(self):
        # Create a fake root cause that suggests schema update
        rc = {
            "suggested_action_types": ["schema_update", "code_patch"],
            "diagnostics": {"missing_columns": ["amount"], "extra_columns": ["amt"]},
        }
        res = generate_patch_for_job(SAMPLE_JOB, rc, self.rules)
        # For our sample job, rename may be applied (conservative), or no change; ensure result shape
        self.assertIn("applied", res)
        self.assertIn("explanation", res)
        self.assertIn("confidence", res)

    def test_oom_broadcast_patch(self):
        # Example: OOM with large shuffle_bytes triggers broadcast insertion if join exists
        rc = {
            "suggested_action_types": ["optimize_join", "code_patch"],
            "diagnostics": {"shuffle_bytes": 2000000000}
        }
        res = generate_patch_for_job(SAMPLE_SKEW_JOB, rc, self.rules)
        self.assertIn("applied", res)
        # applied may be True or False depending on sample file patterns
        self.assertIn("explanation", res)

if __name__ == "__main__":
    unittest.main()
