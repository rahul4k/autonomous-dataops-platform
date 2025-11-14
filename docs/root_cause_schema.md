# Root Cause JSON Schema & Examples

The RCA agent writes `<logfile>.root_cause.json` with the following fields:

- `log_path`: path to the original log file
- `detected_at`: ISO timestamp (UTC)
- `root_cause`: human readable summary of the cause
- `evidence`: list of strings (log lines / short context)
- `suggested_action_types`: list (e.g., ["code_patch","config_change","salting"])
- `diagnostics`: dict with structured diagnostics (e.g., missing_columns, shuffle_bytes, skew_ratio)
- `confidence`: float 0-1
- `remediation_hint`: short actionable sentence
- `classification`: copy of log reader classification JSON

## Example (schema_mismatch)
```json
{
  "log_path": "logs/schema_mismatch/run_2025-11-13_12-10-55.log",
  "detected_at": "2025-11-13T12:05:00Z",
  "root_cause": "Schema mismatch between expected and incoming data fields.",
  "evidence": [
    "SchemaMismatchError: missing columns: ['amount']; extra columns: ['amt']"
  ],
  "suggested_action_types": ["schema_update", "code_patch", "data_provider_alert"],
  "diagnostics": {
    "missing_columns": ["amount"],
    "extra_columns": ["amt"]
  },
  "confidence": 0.92,
  "remediation_hint": "Update StructType/read schema or add tolerant parsing and notify data provider.",
  "classification": {
    "error_type": "SchemaMismatch",
    "error_message": "SchemaMismatchError: missing columns: ['amount']; extra columns: ['amt']",
    "matched_patterns": ["SchemaMismatch"],
    "confidence": 0.9
  }
}

How downstream agents use this

Fixer/Optimizer reads suggested_action_types and diagnostics to pick which code change to propose (e.g., add broadcast join if diagnostics indicate small dimension and large shuffle_bytes).

PR Generator uses root_cause, evidence, confidence to compose PR description and urgency.

Notifier sends remediation_hint and evidence to Slack/Teams for human triage.

---

## 4) Integration notes (how the RCA fits in)
- **Input**: path to log (from event.json published by Airflow or runner) or parsed output from `agents.log_reader_agent.parse_log_file`.
- **Call flow (simple):**
  1. Failure event arrives with `log_path`.
  2. Agent Orchestrator calls `agents.log_reader_agent.parse_log_file(log_path)` → returns `classification`.
  3. Agent Orchestrator calls `agents.root_cause_agent.build_root_cause_json(log_path, classification, full_text=...)` or uses `analyze_log_and_write(log_path)` convenience CLI which calls both.
  4. RCA writes `<log>.root_cause.json`.
  5. Agent Orchestrator then decides next agent based on `suggested_action_types` (e.g., call Fixer for `code_patch` or Notifier for `data_provider_alert`).

- **Where to store**: Keep both `log` and `<log>.root_cause.json` together (same folder). Later copy to GCS path `gs://adp-logs/...` and store metadata row in BigQuery.

- **Confidence gating**: Fixer/PR agent should require a minimum `confidence` (e.g., 0.7) or require extra checks before generating code patches.

---

## 5) How to run and test locally (quick)
1. Ensure you have logs:
```bash
python pipeline/jobs/runner.py schema_mismatch
python pipeline/jobs/runner.py oom
python pipeline/jobs/runner.py skew

2. Run RCA for one log:

python agents/root_cause_agent.py logs/schema_mismatch/run_2025-11-13_12-10-55.log
# Creates logs/schema_mismatch/run_...log.root_cause.json


3. Run test suite (if you installed pytest):

pytest -q tests/test_root_cause_agent.py


4. Open UI to review (optional):

python tools/log_ui.py
# Click the log file and view parsed summary and root cause