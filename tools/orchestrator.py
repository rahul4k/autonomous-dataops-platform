# tools/orchestrator.py
"""
Local Agent Orchestrator for ADP (development/demo)

Workflow:
 - Accepts an event JSON describing a failed job (log_path, job_file)
 - Calls Log Reader Agent -> Root Cause Agent -> AST Fixer -> Fix->PR Orchestrator
 - Prints outcome and (optionally) notifies via Slack webhook

Usage:
 - python tools/orchestrator.py --event event.json
 - or run without args and it will wait for events on stdin (simple)
"""

import argparse
import json
import os
import time
import traceback
from typing import Dict, Any, Optional
from pathlib import Path

# Import existing agents (must exist in repo)
from agents.log_reader_agent import parse_log_file, classify_error
from agents.root_cause_agent import analyze_log_and_write, build_root_cause_json
from agents.ast_fixer import apply_transforms
from agents.fix_pr_orchestrator import orchestrate_fix_and_pr

# Optional: Slack notifier helper
import requests

# Simple logger
def log(msg: str):
    print(f"[orchestrator] {msg}")

def notify_slack(webhook: str, text: str):
    try:
        requests.post(webhook, json={"text": text}, timeout=5)
        log("Slack notification sent")
    except Exception as e:
        log(f"Slack notify failed: {e}")

def handle_event(event: Dict[str, Any], github_token: Optional[str]=None,
                 github_repo: Optional[str]=None, base_branch: str="main",
                 slack_webhook: Optional[str]=None) -> Dict[str, Any]:
    """
    event (expected shape):
    {
      "source": "airflow",
      "dag_id": "...",
      "task_id": "...",
      "run_id": "...",
      "log_path": "logs/.../run_....log",
      "job_file": "pipeline/jobs/xyz.py",   # optional mapping
      "metadata": { ... }
    }
    """

    try:
        log("Received event: " + json.dumps({k: event.get(k) for k in ["dag_id","task_id","run_id","log_path","job_file"]}))

        # 1) Ensure log exists
        log_path = event.get("log_path")
        if not log_path or not os.path.exists(log_path):
            msg = f"log file not found: {log_path}"
            log(msg)
            return {"ok": False, "reason": msg}

        # 2) Log Reader -> classification (returns summary JSON)
        parsed = parse_log_file(log_path)
        classification = parsed.get("summary")
        log(f"Classification: {classification}")

        # 3) Root Cause Analyzer (writes <log>.root_cause.json and returns dict)
        rc = analyze_log_and_write(log_path)
        log(f"RCA produced with confidence={rc.get('confidence')}")

        # 4) Map to job file: prefer event.job_file, else try mapping from task_id
        job_file = event.get("job_file")
        if not job_file:
            # crude mapping: assume pipeline/jobs/<task_id>.py exists
            candidate = os.path.join("pipeline", "jobs", f"{event.get('task_id')}.py")
            if os.path.exists(candidate):
                job_file = candidate
                log(f"Inferred job_file: {job_file}")
            else:
                log("No job_file provided and could not infer. Will stop here and only write RCA.")
                return {"ok": True, "rc": rc, "note": "No job file to patch"}

        # 5) Run AST fixer mapping based on RCA (we call orchestrator's mapper via fix_pr_orchestrator)
        # orchestrate_fix_and_pr handles mapping -> AST fix -> commit -> PR
        if not (github_token and github_repo):
            log("GitHub token/repo not provided. Running AST transforms locally and writing patched artifacts only.")
            # call apply_transforms directly using map from rc
            ast_map = {
                "broadcast": rc.get("suggested_action_types") and any(x.lower().find("broadcast")>=0 for x in rc.get("suggested_action_types")) ,
                "repartition": 200 if ("repartition" in rc.get("suggested_action_types", []) or "salting" in rc.get("suggested_action_types", [])) else None,
                "rename_pair": None
            }
            # attempt to set rename pair
            missing = rc.get("diagnostics", {}).get("missing_columns", [])
            extra = rc.get("diagnostics", {}).get("extra_columns", [])
            if missing and extra:
                ast_map["rename_pair"] = (extra[0], missing[0])

            # Read source, apply transforms and write patched file & patch diff
            source = open(job_file).read()
            patched_src, actions = apply_transforms(source,
                                                    do_broadcast=ast_map["broadcast"],
                                                    do_repartition=ast_map["repartition"],
                                                    rename_pair=ast_map.get("rename_pair"))
            if patched_src == source:
                log("No AST-safe transforms applied locally.")
                return {"ok": True, "rc": rc, "patched": False, "actions": actions}

            patched_path = job_file + ".patched"
            patch_path = job_file + ".patch"
            open(patched_path, "w").write(patched_src)
            from agents.ast_fixer import unified_diff as ast_unified_diff
            diff = ast_unified_diff(source, patched_src, job_file, patched_path)
            open(patch_path, "w").write(diff)
            log(f"Wrote patched file: {patched_path} and patch diff: {patch_path}")
            return {"ok": True, "rc": rc, "patched": True, "patch_path": patch_path, "actions": actions}

        # 6) If GitHub details exist, call the full orchestrator which will create branch, commit and draft PR
        result = orchestrate_fix_and_pr(
            root_cause_path=log_path + ".root_cause.json" if os.path.exists(log_path + ".root_cause.json") else log_path,
            job_file_path=job_file,
            repo=github_repo,
            base_branch=base_branch,
            github_token=github_token
        )

        log(f"Orchestrator result: {result}")

        # 7) Optionally notify Slack
        if slack_webhook and result.get("applied"):
            text = f"ADP produced PR: {result.get('pr_url')} (confidence={result.get('confidence')})"
            notify_slack(slack_webhook, text)

        return {"ok": True, "result": result}

    except Exception as e:
        log("Exception in orchestrator: " + str(e))
        traceback.print_exc()
        return {"ok": False, "error": str(e)}


def run_server_mode(listen_port: int = 8080):
    """
    Minimal HTTP server to receive events (for local development).
    Uses Flask if available, else falls back to simple file-watcher loop (not implemented here).
    """
    try:
        from flask import Flask, request, jsonify
    except Exception:
        log("Flask is not installed. Install with `pip install flask` to use server mode.")
        return

    app = Flask("adp-orchestrator")

    @app.route("/event", methods=["POST"])
    def receive_event():
        payload = request.get_json()
        github_token = os.environ.get("GITHUB_TOKEN")
        github_repo = os.environ.get("GITHUB_REPO")
        slack_webhook = os.environ.get("SLACK_WEBHOOK")
        res = handle_event(payload, github_token=github_token, github_repo=github_repo, slack_webhook=slack_webhook)
        return jsonify(res)

    log(f"Starting orchestrator HTTP server on port {listen_port}")
    app.run(host="0.0.0.0", port=listen_port)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", help="Path to event.json (if omitted runs server mode if flask installed)", default=None)
    parser.add_argument("--github-token", help="GitHub token (optional)", default=None)
    parser.add_argument("--github-repo", help="GitHub repo like owner/repo (optional)", default=None)
    parser.add_argument("--base-branch", help="Base branch for PRs", default="main")
    parser.add_argument("--slack-webhook", help="Slack webhook URL (optional)", default=None)
    parser.add_argument("--server", action="store_true", help="Run as HTTP server to accept events")
    args = parser.parse_args()

    if args.server:
        run_server_mode()
        return

    if args.event:
        ev = json.load(open(args.event))
        res = handle_event(ev, github_token=args.github_token, github_repo=args.github_repo, base_branch=args.base_branch, slack_webhook=args.slack_webhook)
        print(json.dumps(res, indent=2))
        return

    print("No event specified and server mode not enabled. Use --event <path> or --server to start HTTP listener.")


if __name__ == "__main__":
    main()
