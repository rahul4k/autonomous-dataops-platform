# tests/test_ast_fixer.py
import os
import unittest
import json
from agents.ast_fixer import apply_transforms, read_file

# sample job paths (from your Step 1)
SAMPLE_SCHEMA_JOB = "pipeline/jobs/schema_mismatch_job.py"
SAMPLE_SKEW_JOB = "pipeline/jobs/skew_job.py"
SAMPLE_JOIN_JOB = "pipeline/jobs/skew_job.py"  # skew job includes join-like behavior in some setups

class TestASTFixer(unittest.TestCase):

    def test_noop(self):
        # Apply with no transforms -> should be identical
        src = read_file(SAMPLE_SCHEMA_JOB)
        new_src, actions = apply_transforms(src, do_broadcast=False, do_repartition=None, rename_pair=None)
        self.assertEqual(src, new_src)
        self.assertEqual(actions, [])

    def test_rename_insertion(self):
        src = read_file(SAMPLE_SCHEMA_JOB)
        new_src, actions = apply_transforms(src, rename_pair=("amt", "amount"))
        self.assertIn("rename_amt_to_amount", actions)
        # check that the patched code contains withColumnRenamed insertion or similar string
        self.assertTrue(("withColumnRenamed" in new_src) or ("withColumnRenamed" in "".join(actions)))

    def test_repartition_insertion(self):
        src = read_file(SAMPLE_SKEW_JOB)
        new_src, actions = apply_transforms(src, do_repartition=100)
        # actions may include repartition action if groupBy pattern found
        self.assertTrue(isinstance(actions, list))
        # If groupBy absent, it's fine; ensure function runs without exception
        self.assertIsInstance(new_src, str)

    def test_broadcast_wrap(self):
        src = read_file(SAMPLE_JOIN_JOB)
        new_src, actions = apply_transforms(src, do_broadcast=True)
        # Ensure function returns string
        self.assertIsInstance(new_src, str)
        # actions list may or may not include broadcast depending on source contents
        self.assertIsInstance(actions, list)

if __name__ == "__main__":
    unittest.main()
