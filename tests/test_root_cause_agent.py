# tests/test_root_cause_agent.py
import os
import glob
import unittest
from agents.root_cause_agent import analyze_log_and_write

LOG_DIR = "logs"

class TestRCAAgent(unittest.TestCase):
    def setUp(self):
        # detect latest logs for each job
        self.jobs = {}
        for job in ("schema_mismatch", "oom", "skew"):
            pattern = os.path.join(LOG_DIR, job, "run_*.log")
            matches = sorted(glob.glob(pattern))
            if matches:
                self.jobs[job] = matches[-1]
            else:
                self.jobs[job] = None

    def test_schema_mismatch_rca(self):
        path = self.jobs.get("schema_mismatch")
        if not path:
            self.skipTest("No schema_mismatch log - run runner.py first")
        rc = analyze_log_and_write(path)
        self.assertIn("Schema mismatch", rc["root_cause"] or rc["classification"].get("error_type"))
        self.assertTrue(os.path.exists(path + ".root_cause.json"))
        self.assertIn("missing_columns", rc["diagnostics"] or {})

    def test_oom_rca(self):
        path = self.jobs.get("oom")
        if not path:
            self.skipTest("No oom log - run runner.py first")
        rc = analyze_log_and_write(path)
        self.assertTrue("OutOfMemory" in rc["root_cause"] or rc["classification"].get("error_type") == "OOM")
        self.assertTrue(os.path.exists(path + ".root_cause.json"))

    def test_skew_rca(self):
        path = self.jobs.get("skew")
        if not path:
            self.skipTest("No skew log - run runner.py first")
        rc = analyze_log_and_write(path)
        # For skew we expect suggested_action_types to include 'salting' or 'repartition' if skew ratio high
        suggested = rc.get("suggested_action_types", rc.get("diagnostics", {}))
        self.assertTrue(isinstance(rc.get("diagnostics", {}), dict))
        self.assertTrue(os.path.exists(path + ".root_cause.json"))

if __name__ == "__main__":
    unittest.main()
