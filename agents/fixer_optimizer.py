# agents/fixer_optimizer.py
"""
Fixer / Optimizer Agent (rule-based)

Inputs:
  - root_cause_json_path: path to <log>.root_cause.json
  - job_file_path: path to the PySpark job file to patch (optional; can be inferred in advanced flows)

Outputs:
  - <job_file>.patch (unified diff)
  - JSON metadata printed to stdout (patch, explanation, tests_added, confidence)
  - optionally writes tests/test_<jobname>_fix.py

Design notes:
  - Deterministic, rule-based
  - Only suggests patches; does not auto-commit or merge
  - Uses regex-based code transforms for safe, constrained edits
"""

import json
import os
import re
import difflib
from typing import Dict, List, Any, Tuple

RULES_PATH = os.path.join(os.path.dirname(__file__), "fixer_rules.json")


def load_rules(path=RULES_PATH) -> Dict[str, Any]:
    with open(path, "r") as fh:
        return json.load(fh)


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        return fh.read()


def write_file(path: str, content: str):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


def unified_diff(old: str, new: str, old_path: str, new_path: str) -> str:
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)
    diff = difflib.unified_diff(old_lines, new_lines, fromfile=old_path, tofile=new_path)
    return "".join(diff)


# --------------------------
# Simple, safe transformation helpers
# --------------------------

def ensure_broadcast_import(src: str) -> Tuple[str, bool]:
    """
    Ensure `from pyspark.sql import functions as F` or `from pyspark.sql.functions import broadcast` exists.
    Return (new_src, changed)
    """
    changed = False
    if "from pyspark.sql.functions import broadcast" in src:
        return src, False
    # If there's already `import pyspark.sql.functions as F`, add alias use later; prefer explicit import.
    insert_point = None
    # find top of file imports
    lines = src.splitlines()
    for i, ln in enumerate(lines[:40]):  # search first 40 lines
        if ln.strip().startswith("import ") or ln.strip().startswith("from "):
            insert_point = i
    insert_idx = insert_point + 1 if insert_point is not None else 0
    lines.insert(insert_idx, "from pyspark.sql.functions import broadcast")
    changed = True
    return "\n".join(lines) + ("\n" if not src.endswith("\n") else ""), changed


def add_broadcast_to_join(src: str, join_var_regex: str = r"(\w+)\s*=\s*(\w+)\.join\((\w+),\s*on\s*=\s*['\"](\w+)['\"]\)"):
    """
    Find simple join patterns like:
      df = df.join(dim, on='id')
    and replace with:
      from pyspark.sql.functions import broadcast
      df = df.join(broadcast(dim), on='id')
    This is intentionally conservative: only matches simple patterns.
    Returns (new_src, made_change, explanation)
    """
    pattern = re.compile(join_var_regex)
    matches = list(pattern.finditer(src))
    if not matches:
        return src, False, "No simple join pattern found"

    new_src = src
    changes = 0
    for m in reversed(matches):  # reverse to not shift indices
        whole = m.group(0)
        left_var = m.group(1)
        left_df = m.group(2)
        right_df = m.group(3)
        join_key = m.group(4)
        # safety check: avoid replacing if broadcast already used
        if f"broadcast({right_df})" in whole or f"broadcast({left_df})" in whole:
            continue
        replacement = f"{left_var} = {left_df}.join(broadcast({right_df}), on='{join_key}')"
        start, end = m.span(0)
        new_src = new_src[:start] + replacement + new_src[end:]
        changes += 1

    # ensure import
    new_src2, imp_changed = ensure_broadcast_import(new_src)
    made_change = changes > 0 or imp_changed
    explanation = f"Added broadcast wrapping to {changes} join(s); import added={imp_changed}"
    return new_src2, made_change, explanation


def add_repartition_before_groupby(src: str, repartition_count: int = 200):
    """
    Find groupBy patterns and add `.repartition(<n>)` on the DF variable preceding the groupBy.
    E.g. df = df.groupBy('k').agg(...)
    -> df = df.repartition(200).groupBy('k').agg(...)
    Conservative replacement.
    """
    pattern = re.compile(r"(\w+)\s*=\s*(\w+)\.groupBy\(")
    matches = list(pattern.finditer(src))
    if not matches:
        return src, False, "No groupBy pattern found"
    new_src = src
    changes = 0
    for m in reversed(matches):
        whole = m.group(0)
        target_var = m.group(1)
        df_var = m.group(2)
        # Avoid double repartition if recent `.repartition(` exists before `.groupBy`
        # Look back a small window
        start, end = m.span(0)
        snippet_before = new_src[max(0, start-80):start]
        if ".repartition(" in snippet_before:
            continue
        replacement = f"{target_var} = {df_var}.repartition({repartition_count}).groupBy("
        new_src = new_src[:start] + replacement + new_src[end:]
        changes += 1
    explanation = f"Inserted repartition({repartition_count}) before {changes} groupBy(s)"
    return new_src, changes > 0, explanation


def add_column_rename(src: str, old_col: str, new_col: str):
    """
    If job reads JSON and uses old_col, add a `.withColumnRenamed('old', 'new')` chain
    Conservative: find first df assignment and append rename if not present.
    """
    # find first df = spark.read... pattern
    m = re.search(r"(\w+)\s*=\s*spark\.read\..*\)", src)
    if not m:
        # fallback: try to find a df variable assignment
        m2 = re.search(r"(\w+)\s*=", src)
        if not m2:
            return src, False, "No dataframe assignment found to append rename"
        df_var = m2.group(1)
        insert_point = src.find("\n", m2.end()) + 1
        rename_line = f"{df_var} = {df_var}.withColumnRenamed('{old_col}', '{new_col}')\n"
        new_src = src[:insert_point] + rename_line + src[insert_point:]
        return new_src, True, f"Inserted withColumnRenamed('{old_col}','{new_col}')"
    df_var = m.group(1)
    # Avoid duplicate rename
    if f"withColumnRenamed('{old_col}','{new_col}')" in src or f"withColumnRenamed(\"{old_col}\",\"{new_col}\")" in src:
        return src, False, "Rename already present"
    # Find end of the line where df var defined
    start = m.start(0)
    endline = src.find("\n", start)
    insert_point = endline + 1 if endline >= 0 else len(src)
    rename_line = f"{df_var} = {df_var}.withColumnRenamed('{old_col}','{new_col}')\n"
    new_src = src[:insert_point] + rename_line + src[insert_point:]
    return new_src, True, f"Inserted withColumnRenamed('{old_col}','{new_col}')"


# --------------------------
# Orchestration + patch generation
# --------------------------

def choose_actions_from_root_cause(rc: Dict[str, Any]) -> List[str]:
    """
    Map suggested_action_types from RCA into internal actions.
    """
    actions = rc.get("suggested_action_types", [])
    # normalize
    norm = []
    for a in actions:
        norm.extend([x.strip().lower() for x in a.split(",") if x.strip()])
    # dedupe
    return sorted(set(norm))


def generate_patch_for_job(job_path: str, rc: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    """Generate patch applying one or more safe transforms based on RCA."""
    if not os.path.exists(job_path):
        raise FileNotFoundError(f"Job file not found: {job_path}")

    src = read_file(job_path)
    orig = src
    actions = choose_actions_from_root_cause(rc)

    applied_changes = []
    explanations = []
    confidence = 0.5  # base

    # Try specific transforms based on diagnostics and suggested actions
    diag = rc.get("diagnostics", {})

    # 1) Schema fixes: rename columns if missing_columns/extra_columns indicates mapping
    if "schema_update" in actions or "code_patch" in actions:
        missing = diag.get("missing_columns") or []
        extra = diag.get("extra_columns") or []
        # heuristic: if missing contains 'amount' and extra contains 'amt'
        for m in missing:
            for e in extra:
                if e.lower().startswith(m[:3].lower()) or (len(e) >= 3 and e.lower().startswith(m[:3].lower()) == False and (e in src or m in m)):
                    # not a great heuristic; but try rename e->m
                    new_src, changed, exp = add_column_rename(src, e, m)
                    if changed:
                        src = new_src
                        applied_changes.append("rename_column")
                        explanations.append(exp)
                        confidence = max(confidence, rules.get("rename_column_schema", {}).get("confidence_base", 0.9))

    # 2) Broadcast join if shuffle_bytes high and small dim info present
    # RCA may include shuffle_bytes and little dimension info; simple heuristic: if 'shuffle_bytes' large in diagnostics
    shuffle_bytes = diag.get("shuffle_bytes") or 0
    if shuffle_bytes and shuffle_bytes > (1_000_000_000 / 2):  # > ~0.5GB
        # attempt to add broadcast where simple join pattern exists
        new_src, changed, exp = add_broadcast_to_join(src)
        if changed:
            src = new_src
            applied_changes.append("broadcast_join")
            explanations.append(exp)
            confidence = max(confidence, rules.get("broadcast_join", {}).get("confidence_base", 0.8))

    # 3) Skew: add salting hint or repartition
    if "salting" in actions or "repartition" in actions or "salting" in rc.get("suggested_action_types", []):
        new_src, changed, exp = add_repartition_before_groupby(src)
        if changed:
            src = new_src
            applied_changes.append("repartition_before_groupby")
            explanations.append(exp)
            confidence = max(confidence, rules.get("repartition_before_groupby", {}).get("confidence_base", 0.7))

    # 4) If no code change but config suggestion like increase_executor_memory
    if "increase_executor_memory" in actions or "config_change" in actions:
        # We won't change code; we add a comment at top with hint
        hint = "# ADP_HINT: Consider increasing executor memory or driver memory for this job.\n"
        if hint not in src:
            src = hint + src
            applied_changes.append("config_hint_comment")
            explanations.append("Inserted ADP_HINT comment recommending increased executor memory")
            confidence = max(confidence, rules.get("config_hint", {}).get("confidence_base", 0.6))

    # Finalize patch
    if src == orig:
        # No changes applied
        return {
            "applied": False,
            "patch_text": "",
            "explanation": "No safe automated change found for this job based on RCA.",
            "tests_added": [],
            "confidence": 0.0,
            "actions_considered": actions
        }

    # Create patch file content (unified diff)
    patch = unified_diff(orig, src, job_path, job_path + ".patched")
    patch_path = job_path + ".patch"
    write_file(patch_path, patch)

    # Optionally write the patched file to a sandbox path for CI/test-run (not overwriting original)
    patched_path = job_path + ".patched"
    write_file(patched_path, src)

    # Create a minimal unit test stub for the change
    tests_dir = "tests"
    os.makedirs(tests_dir, exist_ok=True)
    job_name = os.path.splitext(os.path.basename(job_path))[0]
    test_filename = os.path.join(tests_dir, f"test_{job_name}_fix.py")
    test_content = f"""# Auto-generated test stub by ADP Fixer
import pytest
def test_{job_name}_sanity():
    # This is a placeholder test. Replace with real functional tests.
    assert True, "Replace with functional assertions for {job_name}"
"""
    write_file(test_filename, test_content)

    return {
        "applied": True,
        "patch_path": patch_path,
        "patched_file_path": patched_path,
        "patch_text": patch,
        "explanation": "; ".join(explanations),
        "tests_added": [test_filename],
        "confidence": round(min(0.99, confidence), 2),
        "actions_considered": actions
    }


def cli_main():
    import argparse, pprint
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-cause", required=True, help="Path to root_cause.json")
    parser.add_argument("--job-file", required=True, help="Path to job file to patch")
    args = parser.parse_args()

    rules = load_rules()
    # Read root cause
    with open(args.root_cause, "r") as fh:
        rc = json.load(fh)

    try:
        result = generate_patch_for_job(args.job_file, rc, rules)
    except Exception as e:
        result = {"applied": False, "error": str(e)}

    pprint.pprint(result)
    # Write result metadata
    meta_path = args.job_file + ".adp_patch.json"
    with open(meta_path, "w") as fh:
        json.dump(result, fh, indent=2)
    print("Wrote metadata to", meta_path)


if __name__ == "__main__":
    cli_main()
