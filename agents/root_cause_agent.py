# agents/root_cause_agent.py
"""
Root Cause Analyzer (RCA) Agent

- Input: a log file path OR the parsed output from agents/log_reader_agent.parse_log_file()
- Output: writes <logfile>.root_cause.json and returns the JSON object.

Design:
- Uses heuristics + lightweight profiling on the log excerpt and metadata.
- Produces a structured `root_cause` JSON including:
  - root_cause (human readable)
  - evidence (list of strings)
  - suggested_action_types (list)
  - diagnostics (structured fields per failure type)
  - confidence (0-1)
  - remediation_hint (short actionable item)
- Deterministic and testable; can later be extended with LLM inference.
"""

import os
import json
import re
import yaml
from datetime import datetime
from typing import Dict, Any, Optional

from agents.log_reader_agent import read_log_file, classify_error

def load_config(path="config.yaml"):
    if os.path.exists(path):
        with open(path, "r") as f:
            return yaml.safe_load(f)
    return {}

CONFIG = load_config()
RCA_CONFIG = CONFIG.get("root_cause", {})
THRESHOLDS = RCA_CONFIG.get("thresholds", {})

# Thresholds & heuristics
SKEW_RATIO_THRESHOLD = THRESHOLDS.get("skew_ratio", 0.5)
BROADCAST_SIZE_THRESHOLD_BYTES = THRESHOLDS.get("broadcast_size_bytes", 100 * 1024 * 1024)
SHUFFLE_BYTES_HIGH = THRESHOLDS.get("shuffle_bytes_high", 1_000_000_000)

def _safe_read(path: str, max_lines: int = 3000) -> str:
    try:
        return read_log_file(path, max_lines=max_lines)
    except Exception as e:
        return f"Could not read log file: {e}"

def _extract_numbers(text: str):
    """Return list of numeric tokens found in text (integers)."""
    return [int(x) for x in re.findall(r"\b(\d{2,})\b", text)]  # crude: numbers with 2+ digits

def _find_shuffle_bytes(text: str) -> Optional[int]:
    # look for patterns like shuffle_bytes=123456789 or Estimated shuffle_bytes ~ 12345
    m = re.search(r"shuffle[_ ]?bytes[^0-9]*([0-9]+)", text, re.I)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    m2 = re.search(r"Estimated shuffle_bytes ~\s*([0-9,]+)", text, re.I)
    if m2:
        try:
            return int(m2.group(1).replace(",", ""))
        except ValueError:
            return None
    return None

def analyze_schema_mismatch(text: str, classification: Dict) -> Dict[str, Any]:
    diagnostics = {}
    evidence = []
    # Try to parse "missing columns" style lines
    m = re.search(r"missing columns[:\s]*\[?([^\]]+)\]?", text, re.I)
    if m:
        cols = [c.strip(" '\"") for c in m.group(1).split(",")]
        diagnostics["missing_columns"] = cols
        evidence.append(f"Detected missing columns: {cols}")
    m2 = re.search(r"extra columns[:\s]*\[?([^\]]+)\]?", text, re.I)
    if m2:
        cols = [c.strip(" '\"") for c in m2.group(1).split(",")]
        diagnostics["extra_columns"] = cols
        evidence.append(f"Detected extra columns: {cols}")

    # Fallback: look for 'amt' vs 'amount' pattern in logs (demo-specific)
    if "amt" in text and "amount" not in text:
        diagnostics.setdefault("extra_columns", []).append("amt")
        evidence.append("Found 'amt' in data; expected 'amount' (demo pattern)")

    confidence = 0.9 if diagnostics else 0.6
    return {
        "root_cause": "Schema mismatch between expected and incoming data fields.",
        "evidence": evidence or [classification.get("error_message", "Schema mismatch suspected")],
        "suggested_action_types": ["schema_update", "code_patch", "data_provider_alert"],
        "diagnostics": diagnostics,
        "confidence": round(confidence, 2),
        "remediation_hint": "Update StructType/read schema or add tolerant parsing and notify data provider."
    }

def analyze_oom(text: str, classification: Dict) -> Dict[str, Any]:
    diagnostics = {}
    evidence = []
    # look for heap/Java OOM specifics
    if re.search(r"OutOfMemoryError|heap space|GC overhead", text, re.I):
        evidence.append("Detected Java/Python OutOfMemoryError in logs.")
    # try to find executor id or driver id
    m = re.search(r"(executor_\d+|driver)_?(\d+)?", text, re.I)
    if m:
        diagnostics["instance_hint"] = m.group(0)
        evidence.append(f"Instance hint: {m.group(0)}")

    # try to find shuffle bytes
    shuffle = _find_shuffle_bytes(text)
    if shuffle:
        diagnostics["shuffle_bytes"] = shuffle
        evidence.append(f"shuffle_bytes={shuffle}")

    # Set confidence based on evidence
    confidence = 0.9 if evidence else 0.6
    # Suggest actions
    hints = []
    if shuffle and shuffle > SHUFFLE_BYTES_HIGH:
        hints.append("Large shuffle detected; consider broadcast join or repartitioning.")
        suggested = ["config_change", "code_patch", "optimize_join"]
    else:
        suggested = ["config_change", "increase_executor_memory", "optimize_transformations"]

    return {
        "root_cause": "OutOfMemory (OOM) encountered during job execution.",
        "evidence": evidence or [classification.get("error_message", "OOM suspected")],
        "suggested_action_types": suggested,
        "diagnostics": diagnostics,
        "confidence": round(confidence, 2),
        "remediation_hint": "Consider increasing executor memory, reducing shuffle, or using broadcast joins for small dimension tables."
    }

def analyze_skew(text: str, classification: Dict) -> Dict[str, Any]:
    diagnostics = {}
    evidence = []
    # look for hotkey/skew ratio lines like "hotkey_count=95000" or "skew_ratio=0.95"
    m = re.search(r"skew_ratio\s*=\s*([0-9]*\.?[0-9]+)", text, re.I)
    if m:
        ratio = float(m.group(1))
        diagnostics["skew_ratio"] = ratio
        evidence.append(f"Detected skew_ratio={ratio}")
    else:
        # fallback: parse counts printed in our demo (hotkey_count=...)
        m2 = re.search(r"hotkey_count\s*=\s*([0-9]+)", text, re.I)
        m3 = re.search(r"total_records\s*=\s*([0-9]+)", text, re.I)
        if m2 and m3:
            hot = int(m2.group(1)); total = int(m3.group(1))
            diagnostics["hotkey_count"] = hot
            diagnostics["total_records"] = total
            diagnostics["skew_ratio"] = round(hot/total, 4) if total else None
            evidence.append(f"hotkey_count={hot}; total_records={total}")

    # heuristics
    skew_ratio = diagnostics.get("skew_ratio")
    if skew_ratio is not None and skew_ratio > SKEW_RATIO_THRESHOLD:
        confidence = 0.9
        suggested = ["code_patch", "salting", "repartition"]
        remediation = "Add salting on the join key or pre-aggregate hot partitions; consider repartitioning before join."
    else:
        confidence = 0.5
        suggested = ["monitor", "investigate"]
        remediation = "Skew suspected; collect more metrics."

    # try to find shuffle bytes too
    shuffle = _find_shuffle_bytes(text)
    if shuffle:
        diagnostics["shuffle_bytes"] = shuffle
        evidence.append(f"shuffle_bytes={shuffle}")
        if shuffle > SHUFFLE_BYTES_HIGH:
            suggested.append("broadcast_or_repartition")

    return {
        "root_cause": "Data skew on join key leading to performance hotspot.",
        "evidence": evidence or [classification.get("error_message", "Skew suspected")],
        "suggested_action_types": suggested,
        "diagnostics": diagnostics,
        "confidence": round(confidence, 2),
        "remediation_hint": remediation
    }

def analyze_missing_file(text: str, classification: Dict) -> Dict[str, Any]:
    evidence = []
    m = re.search(r"NoSuchFileException|FileNotFoundException|File not found|not found:", text, re.I)
    if m:
        evidence.append(m.group(0))
    # try to extract path
    m2 = re.search(r"(/[\w\-\./]+)", text)
    diagnostics = {}
    if m2:
        diagnostics["missing_path"] = m2.group(1)
        evidence.append(f"Missing path: {m2.group(1)}")
    return {
        "root_cause": "Input file or resource not found.",
        "evidence": evidence or [classification.get("error_message", "Missing file/resource")],
        "suggested_action_types": ["data_availability_check", "alert", "retry"],
        "diagnostics": diagnostics,
        "confidence": 0.85,
        "remediation_hint": "Verify upstream data landing, storage permissions, and retry logic."
    }

def analyze_auth_error(text: str, classification: Dict) -> Dict[str, Any]:
    evidence = []
    m = re.search(r"Auth|Authentication|permission denied|403|AccessDenied|401", text, re.I)
    if m:
        evidence.append(m.group(0))
    return {
        "root_cause": "Authentication/Authorization error when accessing resource.",
        "evidence": evidence or [classification.get("error_message", "Auth error detected")],
        "suggested_action_types": ["secrets_check", "credential_rotation", "alert"],
        "diagnostics": {},
        "confidence": 0.9,
        "remediation_hint": "Check service account permissions, token expiry and secret validity."
    }

def analyze_gc(text: str, classification: Dict) -> Dict[str, Any]:
    evidence = []
    if re.search(r"GC overhead|Full GC|garbage collection", text, re.I):
        evidence.append("Garbage collection pressure detected.")
    return {
        "root_cause": "Frequent garbage collection causing performance degradation and possible OOM.",
        "evidence": evidence or [classification.get("error_message", "GC issues suspected")],
        "suggested_action_types": ["config_change", "tune_gc", "increase_memory"],
        "diagnostics": {},
        "confidence": 0.8,
        "remediation_hint": "Tune JVM GC or increase executor memory; investigate large object retention."
    }

def analyze_other(text: str, classification: Dict) -> Dict[str, Any]:
    # Generic fallback; include first exception line and a few surrounding lines as evidence
    lines = text.splitlines()
    first_exc = None
    for i, ln in enumerate(lines):
        if re.search(r"Exception|Error|Traceback|PerformanceAlert|OutOfMemoryError", ln, re.I):
            first_exc = (i, ln)
            break
    evidence = []
    if first_exc:
        i, ln = first_exc
        context = "\n".join(lines[max(0, i-2):i+3])
        evidence.append(context)
    else:
        evidence.append(classification.get("error_message", "No explicit error line found"))

    return {
        "root_cause": "Unknown / unclassified error. Manual investigation required.",
        "evidence": evidence,
        "suggested_action_types": ["manual_review", "collect_more_data"],
        "diagnostics": {},
        "confidence": 0.4,
        "remediation_hint": "Open for manual investigation and attach more logs or metrics."
    }

ANALYSIS_DISPATCH = {
    "SchemaMismatch": analyze_schema_mismatch,
    "OOM": analyze_oom,
    "Skew": analyze_skew,
    "MissingFile": analyze_missing_file,
    "AuthError": analyze_auth_error,
    "GC": analyze_gc,
    "Other": analyze_other
}

def build_root_cause_json(log_path: str, classification: Dict[str, Any], full_text: Optional[str] = None) -> Dict[str, Any]:
    text = full_text or _safe_read(log_path, max_lines=3000)
    err_type = classification.get("error_type", "Other")
    analyzer = ANALYSIS_DISPATCH.get(err_type, analyze_other)
    analysis = analyzer(text, classification)

    rc = {
        "log_path": log_path,
        "detected_at": datetime.utcnow().isoformat() + "Z",
        "root_cause": analysis["root_cause"],
        "evidence": analysis.get("evidence", []),
        "suggested_action_types": analysis.get("suggested_action_types", []),
        "diagnostics": analysis.get("diagnostics", {}),
        "confidence": analysis.get("confidence", 0.5),
        "remediation_hint": analysis.get("remediation_hint", ""),
        "classification": classification
    }
    return rc

def write_root_cause_json(log_path: str, rc_json: Dict[str, Any]) -> str:
    out_path = log_path + ".root_cause.json"
    with open(out_path, "w") as fh:
        json.dump(rc_json, fh, indent=2)
    return out_path

def analyze_log_and_write(log_path: str) -> Dict[str, Any]:
    text = _safe_read(log_path, max_lines=3000)
    classification = classify_error(text)
    rc = build_root_cause_json(log_path, classification, full_text=text)
    out = write_root_cause_json(log_path, rc)
    return rc

# CLI convenience
if __name__ == "__main__":
    import argparse, pprint
    parser = argparse.ArgumentParser()
    parser.add_argument("logfile", help="path to a saved log file")
    args = parser.parse_args()
    rc = analyze_log_and_write(args.logfile)
    pprint.pprint(rc)
    print("Wrote root cause JSON to:", args.logfile + ".root_cause.json")
