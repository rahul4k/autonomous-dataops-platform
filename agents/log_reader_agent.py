# agents/log_reader_agent.py
"""
Simple Log Reader Agent.
- Reads a log file path (local).
- Classifies failure into categories with regex+heuristics.
- Returns structured JSON suitable for downstream RCA agent.
Notes
- This is intentionally rule-based (regexes) so tests are deterministic.
- Later, you can replace classify_error body with LLM-based classification that reads the same inputs.
"""

import re
import json
import os
import yaml
from typing import Dict, List

def load_config(path="config.yaml"):
    if os.path.exists(path):
        with open(path, "r") as f:
            return yaml.safe_load(f)
    return {}

CONFIG = load_config()
LOG_READER_CONFIG = CONFIG.get("log_reader", {})
PATTERNS_CONFIG = LOG_READER_CONFIG.get("patterns", {})

# Precompiled patterns
_PATTERNS = {
    name: re.compile(pattern, re.I)
    for name, pattern in PATTERNS_CONFIG.items()
}

# Fallback if config is missing
if not _PATTERNS:
    _PATTERNS = {
        "SchemaMismatch": re.compile(r"(SchemaMismatch|missing columns|missing column|unexpected column)", re.I),
        "OOM": re.compile(r"OutOfMemoryError|OOM|java.lang.OutOfMemoryError|GC overhead|heap space", re.I),
        "Skew": re.compile(r"skew|hotkey|skew_ratio|PerformanceAlert", re.I),
        "GC": re.compile(r"GC overhead|full gc|garbage collection", re.I),
        "MissingFile": re.compile(r"(NoSuchFileException|FileNotFoundException|File not found|not found:)", re.I),
        "AuthError": re.compile(r"Auth|Authentication|permission denied|403|AccessDenied", re.I),
    }

def read_log_file(path: str, max_lines: int = 3000) -> str:
    """Read log file and return last max_lines of content as a single string."""
    max_lines = LOG_READER_CONFIG.get("max_lines", max_lines)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        # keep only last N lines so agent works with bounded size
        trimmed = lines[-max_lines:]
        return "".join(trimmed)
    except Exception as e:
        print(f"Error reading log file {path}: {e}")
        return ""

def classify_error(log_text: str) -> Dict:
    """
    Classify the given log text into an error type.
    Returns structured JSON:
    {
      "error_type": <one-of>,
      "error_message": "<best-match snippet or first exception line>",
      "matched_patterns": [...],
      "confidence": 0-1
    }
    """
    matches: List[str] = []
    for name, pat in _PATTERNS.items():
        if pat.search(log_text):
            matches.append(name)

    # Find the first exception-like line to include as message
    first_exception = None
    for line in log_text.splitlines():
        if re.search(r"Exception|Error|Traceback|PerformanceAlert|OutOfMemoryError", line, re.I):
            first_exception = line.strip()
            break

    if not matches:
        error_type = "Other"
        confidence = 0.45
    else:
        # Choose highest priority match: OOM > SchemaMismatch > Skew > MissingFile > AuthError > GC
        priority = ["OOM", "SchemaMismatch", "Skew", "MissingFile", "AuthError", "GC"]
        chosen = None
        for p in priority:
            if p in matches:
                chosen = p
                break
        error_type = chosen or matches[0]
        # crude confidence heuristic
        confidence = min(0.9, 0.6 + 0.1 * (len(matches)-1))

    return {
        "error_type": error_type,
        "error_message": first_exception or "No explicit exception line found",
        "matched_patterns": matches,
        "confidence": round(confidence, 2),
    }

def parse_log_file(path: str) -> Dict:
    """High-level helper: read file and classify, plus metadata."""
    text = read_log_file(path)
    classification = classify_error(text)
    return {
        "log_path": path,
        "summary": classification,
        "sample_excerpt": text[:4000],   # store small excerpt for quick view
    }

if __name__ == "__main__":
    import argparse, pprint
    parser = argparse.ArgumentParser()
    parser.add_argument("logfile", help="path to log file")
    args = parser.parse_args()
    out = parse_log_file(args.logfile)
    pprint.pprint(out)
    # Also dump JSON next to file as <logfile>.rca.json for convenience (optional)
    with open(args.logfile + ".rca.json", "w") as fh:
        json.dump(out["summary"], fh, indent=2)
    print("Wrote RCA JSON to", args.logfile + ".rca.json")
