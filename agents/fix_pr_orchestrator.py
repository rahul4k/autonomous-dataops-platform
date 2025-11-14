# agents/fix_pr_orchestrator.py
"""
Fix → PR Orchestrator for Autonomous DataOps Platform.

Pipeline:
1. Read root_cause.json
2. Convert RCA diagnostics → AST Fixer actions
3. Apply AST-based code modifications
4. Run compile check on patched file
5. Create a GitHub branch + commit patched file
6. Open a Draft Pull Request

Requires:
- agents.ast_fixer (apply_transforms, read_file, write_file, unified_diff)
- agents.fixer_optimizer (choose_actions_from_root_cause)
"""

import os
import json
import uuid
import base64
import subprocess
import requests

from agents.ast_fixer import apply_transforms, read_file, write_file, unified_diff
from agents.fixer_optimizer import choose_actions_from_root_cause


# ---------------------------------------------------------
# RCA → AST action mapping
# ---------------------------------------------------------

def map_rca_to_ast_actions(rc: dict) -> dict:
    actions = choose_actions_from_root_cause(rc)

    do_broadcast = False
    repartition_n = None
    rename_pair = None

    diag = rc.get("diagnostics", {})
    missing_cols = diag.get("missing_columns", []) or []
    extra_cols = diag.get("extra_columns", []) or []

    if ("schema_update" in actions or "code_patch" in actions) and missing_cols and extra_cols:
        rename_pair = (extra_cols[0], missing_cols[0])

    if "repartition" in actions or "salting" in actions:
        repartition_n = 200

    if "optimize_join" in actions or "broadcast_or_repartition" in actions:
        do_broadcast = True

    return {
        "broadcast": do_broadcast,
        "repartition": repartition_n,
        "rename": rename_pair,
        "actions_from_rca": actions
    }


# ---------------------------------------------------------
# GitHub helpers
# ---------------------------------------------------------

def github_get_sha(repo: str, branch: str, token: str) -> str:
    url = f"https://api.github.com/repos/{repo}/git/ref/heads/{branch}"
    headers = {"Authorization": f"token {token}"}

    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Failed to fetch branch SHA: {resp.text}")

    return resp.json()["object"]["sha"]


def github_create_branch(repo: str, new_branch: str, base_sha: str, token: str):
    url = f"https://api.github.com/repos/{repo}/git/refs"
    headers = {"Authorization": f"token {token}"}
    payload = {"ref": f"refs/heads/{new_branch}", "sha": base_sha}

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to create branch: {resp.text}")


def github_commit_file(repo: str, branch: str, path: str, content: str, token: str):
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"Authorization": f"token {token}"}

    resp = requests.get(url, headers=headers, params={"ref": branch})
    sha = resp.json().get("sha") if resp.status_code == 200 else None

    payload = {
        "message": f"[ADP] Automated fix: {path}",
        "content": base64.b64encode(content.encode()).decode(),
        "branch": branch
    }

    if sha:
        payload["sha"] = sha

    resp2 = requests.put(url, headers=headers, json=payload)
    if resp2.status_code not in (200, 201):
        raise Exception(f"Failed to commit: {resp2.text}")


def github_create_pr(repo: str, head_branch: str, base_branch: str,
                     title: str, body_md: str, token: str) -> str:
    url = f"https://api.github.com/repos/{repo}/pulls"
    headers = {"Authorization": f"token {token}"}

    payload = {
        "title": title,
        "body": body_md,
        "head": head_branch,
        "base": base_branch,
        "draft": True
    }

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to create PR: {resp.text}")

    return resp.json()["html_url"]


# ---------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------

def orchestrate_fix_and_pr(
    root_cause_path: str,
    job_file_path: str,
    repo: str,
    base_branch: str,
    github_token: str
) -> dict:

    if not os.path.exists(root_cause_path):
        raise FileNotFoundError(f"Root cause file missing: {root_cause_path}")

    with open(root_cause_path) as f:
        rc = json.load(f)

    flags = map_rca_to_ast_actions(rc)

    original_code = read_file(job_file_path)
    patched_code, applied_actions = apply_transforms(
        original_code,
        do_broadcast=flags["broadcast"],
        do_repartition=flags["repartition"],
        rename_pair=flags["rename"]
    )

    if patched_code == original_code:
        return {
            "applied": False,
            "reason": "No safe AST transforms applicable.",
            "actions_considered": applied_actions
        }

    # Write patched file
    patched_path = job_file_path + ".patched"
    write_file(patched_path, patched_code)

    # Write patch diff
    patch_text = unified_diff(original_code, patched_code, job_file_path, patched_path)
    patch_path = job_file_path + ".patch"
    write_file(patch_path, patch_text)

    # Compile sanity-check
    proc = subprocess.run(["python", "-m", "py_compile", patched_path], capture_output=True)
    if proc.returncode != 0:
        return {
            "applied": False,
            "reason": "Patched file failed compile check.",
            "stderr": proc.stderr.decode()
        }

    # Create git branch
    new_branch = f"adp-fix-{uuid.uuid4().hex[:6]}"
    base_sha = github_get_sha(repo, base_branch, github_token)
    github_create_branch(repo, new_branch, base_sha, github_token)

    # Commit patched code
    github_commit_file(
        repo=repo,
        branch=new_branch,
        path=job_file_path,
        content=patched_code,
        token=github_token
    )

    # PR body
    confidence = rc.get("confidence", 0.75)
    pr_body = f"""
    ## 🤖 ADP Automated Fix

    ### Root Cause
    {rc.get("root_cause")}

    ### Applied Actions
    {json.dumps(applied_actions, indent=2)}

    ### Diagnostics
    {json.dumps(rc.get("diagnostics", {}), indent=2)}

    ### Confidence
    {confidence}

    ### Patch Diff
    ```diff
    {patch_text}
    This is a draft PR created automatically by ADP.
    """
    
    # Create PR
    pr_url = github_create_pr(
        repo=repo,
        head_branch=new_branch,
        base_branch=base_branch,
        title=f"[ADP] Automated Fix: {os.path.basename(job_file_path)}",
        body_md=pr_body,
        token=github_token
    )

    return {
        "applied": True,
        "branch_created": new_branch,
        "patch_path": patch_path,
        "pr_url": pr_url,
        "confidence": confidence,
        "actions": applied_actions
    }

if __name__ == "main":
    import argparse
    import pprint
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-cause", required=True)
    parser.add_argument("--job-file", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--branch", default="main")
    parser.add_argument("--token", required=True)

    args = parser.parse_args()

    result = orchestrate_fix_and_pr(
        root_cause_path=args.root_cause,
        job_file_path=args.job_file,
        repo=args.repo,
        base_branch=args.branch,
        github_token=args.token
    )

    pprint.pprint(result)
