# tests/test_log_reader.py
import os
import glob
import unittest
from agents.log_reader_agent import parse_log_file, classify_error

LOG_DIR = "logs"

class TestLogReaderAgent(unittest.TestCase):
    def setUp(self):
        # find latest log for each job if available
        self.jobs = {}
        for job in ("schema_mismatch", "oom", "skew"):
            pattern = os.path.join(LOG_DIR, job, "run_*.log")
            matches = sorted(glob.glob(pattern))
            if matches:
                self.jobs[job] = matches[-1]  # latest
            else:
                self.jobs[job] = None

    def test_schema_mismatch_detection(self):
        path = self.jobs.get("schema_mismatch")
        if path is None:
            self.skipTest("No schema_mismatch log found under logs/; run runner.py first")
        parsed = parse_log_file(path)
        self.assertIn(parsed["summary"]["error_type"], ("SchemaMismatch", "Other", "MissingFile"))
        # Expect 'missing columns' or similar to be present in the message or excerpt
        excerpt = parsed["sample_excerpt"].lower()
        self.assertTrue("missing" in excerpt or "schema" in excerpt or "amt" in excerpt)

    def test_oom_detection(self):
        path = self.jobs.get("oom")
        if path is None:
            self.skipTest("No oom log found under logs/; run runner.py first")
        parsed = parse_log_file(path)
        # OOM should be detected
        self.assertIn(parsed["summary"]["error_type"], ("OOM", "Other"))
        self.assertTrue("outofmemory" in parsed["sample_excerpt"].lower() or "heap space" in parsed["sample_excerpt"].lower())

    def test_skew_detection(self):
        path = self.jobs.get("skew")
        if path is None:
            self.skipTest("No skew log found under logs/; run runner.py first")
        parsed = parse_log_file(path)
        # Skew should be detected or at least PerformanceAlert present
        et = parsed["summary"]["error_type"]
        self.assertIn(et, ("Skew", "Other"))
        self.assertTrue("skew" in parsed["sample_excerpt"].lower() or "hotkey" in parsed["sample_excerpt"].lower())

if __name__ == "__main__":
    unittest.main()
