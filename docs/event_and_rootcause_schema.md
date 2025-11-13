# Event & Root Cause JSON Schemas

This document defines the JSON schemas for failure events and root cause analysis in the Autonomous DataOps Platform (ADP).

## 1. Failure Event (event.json)

An event that Airflow/job runner will publish to Pub/Sub or your local simulator when a pipeline task fails.

### Schema Definition

```json
{
  "source": "airflow",
  "project": "adp-staging",
  "dag_id": "adp_sample_failures",
  "task_id": "schema_mismatch_job",
  "run_id": "manual__2025-11-13T12:00:00",
  "attempt": 1,
  "timestamp": "2025-11-13T12:00:00Z",
  "log_path": "logs/schema_mismatch/run_2025-11-13_12-10-55.log",
  "metadata": {
    "owner": "adp-demo",
    "env": "local"
  }
}
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Source system (e.g., "airflow", "cloud-composer", "kubernetes") |
| `project` | string | GCP project ID or environment name |
| `dag_id` | string | Airflow DAG identifier |
| `task_id` | string | Failed task identifier within the DAG |
| `run_id` | string | Unique run identifier (timestamp-based) |
| `attempt` | integer | Retry attempt number (1-based) |
| `timestamp` | string | ISO 8601 timestamp when failure occurred |
| `log_path` | string | Path to log file (GCS, local, or absolute) |
| `metadata` | object | Optional metadata (owner, environment, tags, etc.) |

---

## 2. Root Cause JSON (root_cause.json)

Produced by the Root Cause Analyzer agent. Stored beside the log file as `<logfile>.root_cause.json`.

### Schema Definition

```json
{
  "log_path": "logs/schema_mismatch/run_2025-11-13_12-10-55.log",
  "detected_at": "2025-11-13T12:05:00Z",
  "root_cause": "Missing column 'amount' in incoming JSON - incoming contains 'amt' instead. Job expected field 'amount' and is configured with strict schema.",
  "evidence": [
    "SchemaMismatchError: missing columns: ['amount']; extra columns: ['amt']"
  ],
  "suggested_action_types": [
    "code_patch",
    "schema_update",
    "data_provider_alert"
  ],
  "diagnostics": {
    "expected_columns": [
      "user_id",
      "name",
      "created_at",
      "amount"
    ],
    "incoming_sample_columns": [
      "user_id",
      "name",
      "created_at",
      "amt"
    ],
    "missing_columns": [
      "amount"
    ],
    "extra_columns": [
      "amt"
    ]
  },
  "confidence": 0.92
}
```

### Field Descriptions

| Field | Type | Description |
|-------|------|-------------|
| `log_path` | string | Path to the associated log file |
| `detected_at` | string | ISO 8601 timestamp when RCA detected the root cause |
| `root_cause` | string | Human-readable explanation of the root cause |
| `evidence` | array[string] | List of log lines or error messages supporting the diagnosis |
| `suggested_action_types` | array[string] | Suggested remediation actions ("code_patch", "schema_update", "config_change", "data_provider_alert", etc.) |
| `diagnostics` | object | Detailed diagnostic information specific to the error type |
| `confidence` | number | Confidence score (0.0 to 1.0) |

---

## 3. Usage & Integration

### Event Publishing Flow

1. **Airflow/Job fails** → Airflow task logs to GCS or local storage
2. **Failure detection** → Airflow publishes `event.json` to Pub/Sub topic
3. **Log Reader Agent** → Subscribes to Pub/Sub, fetches log file
4. **Root Cause Analyzer** → Analyzes logs, produces `root_cause.json`
5. **Fixer & Optimizer** → Reads `root_cause.json`, proposes patches
6. **PR Generator** → Creates PRs with fixes
7. **Notifier** → Sends alerts to team

### File Organization

```
logs/
├── schema_mismatch/
│   ├── run_2025-11-13_12-10-55.log
│   └── run_2025-11-13_12-10-55.log.root_cause.json  <-- RCA output
├── missing_dependency/
│   ├── run_2025-11-13_13-20-30.log
│   └── run_2025-11-13_13-20-30.log.root_cause.json
└── ...
```

### Key Principles

- **event.json** is the single source of truth for triggering all ADP agents
- **root_cause.json** is stored beside the log file for easy reference
- **root_cause.json** is referenced in generated PRs and monitoring dashboards
- All timestamps are in UTC (ISO 8601 format)
- Confidence scores must be between 0.0 and 1.0 (decimal format)
- All JSON must be valid and parseable by agents

---

## 4. Example Scenarios

### Scenario 1: Schema Mismatch

**event.json** triggers RCA → **root_cause.json** contains:
- Expected vs. incoming column mismatch
- Suggested action: schema_update or code_patch
- Confidence: 0.92

### Scenario 2: Missing Dependency

**root_cause.json** might contain:
```json
{
  "root_cause": "Python library 'requests==2.25.1' not found in requirements.txt",
  "suggested_action_types": ["code_patch"],
  "diagnostics": {
    "missing_package": "requests",
    "required_version": "2.25.1"
  },
  "confidence": 0.95
}
```

### Scenario 3: Resource Exhaustion

**root_cause.json** might contain:
```json
{
  "root_cause": "Job exceeded allocated memory (4GB). Last record count: 12M rows",
  "suggested_action_types": ["config_change"],
  "diagnostics": {
    "allocated_memory_gb": 4,
    "actual_memory_used_gb": 4.2,
    "last_record_count": 12000000
  },
  "confidence": 0.88
}
```

---

## 5. Best Practices

1. **Always populate `evidence`** with exact error messages from logs
2. **Keep `root_cause` concise** but informative (1-2 sentences)
3. **Use realistic confidence scores** - don't always return 1.0
4. **Include diagnostic context** - help PR generator understand severity
5. **Suggest multiple action types** when applicable
6. **Timestamp synchronization** - ensure event and RCA timestamps are in UTC
7. **Validate JSON output** - agents must output valid, parseable JSON

---

## 6. Related Documentation

- See `docs/agent_prompts.md` for agent system prompts and instructions
- See `docs/architecture.md` for system design and data flow
- See `docs/quickstart.md` for setup and local testing
