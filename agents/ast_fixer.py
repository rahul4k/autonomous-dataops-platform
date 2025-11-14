# agents/ast_fixer.py
"""
AST-based Fixer using libcst.

Transforms implemented (conservative):
- Wrap right-side of simple .join(...) calls with broadcast(...) and insert import if missing
- Insert .repartition(N) before .groupBy(...) calls on DataFrame expressions
- Insert withColumnRenamed('old','new') statement after a spark.read.* assignment (conservative)

Usage (CLI):
python agents/ast_fixer.py --job-file pipeline/jobs/schema_mismatch_job.py \
    --rename old_col:new_col --repartition 200 --broadcast

Outputs:
- <job_file>.patched  (patched source)
- <job_file>.patch   (unified diff between original and patched)
- prints JSON metadata to stdout
"""

import argparse
import json
import os
import difflib
from typing import Optional, Tuple, List

import libcst as cst
import libcst.matchers as m

# -----------------------
# Helpers
# -----------------------

def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        return fh.read()

def write_file(path: str, content: str):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)

def unified_diff(orig: str, new: str, orig_path: str, new_path: str) -> str:
    return "".join(difflib.unified_diff(orig.splitlines(keepends=True),
                                        new.splitlines(keepends=True),
                                        fromfile=orig_path, tofile=new_path))

# -----------------------
# Transformers
# -----------------------

class BroadcastJoinTransformer(cst.CSTTransformer):
    """
    Wrap simple join right-hand argument in broadcast(...) if join is of form:
      <left> = <left_df>.join(<right_df>, on='key')
    This transformer will:
      - Replace join call arg with broadcast(<right_df>) for simple Name or Attribute args.
      - Record whether change occurred.
    """
    def __init__(self):
        self.changed = False

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        # We look for .join calls: attribute value endswith .join
        func = original_node.func
        if m.matches(func, m.Attribute(value=m.OneOf(m.Name(), m.Attribute()), attr=m.Name("join"))):
            # Expect first arg positional or keyword 'on'
            args = list(updated_node.args)
            if not args:
                return updated_node
            # If the first arg is a simple Name or Attribute, wrap it
            first = args[0].value
            if m.matches(first, m.OneOf(m.Name(), m.Attribute())):
                # if it's already broadcast(...) do nothing
                if m.matches(first, m.Call(func=m.Name("broadcast"))):
                    return updated_node
                # build broadcast(<first>)
                broadcast_call = cst.Call(func=cst.Name("broadcast"), args=[cst.Arg(first)])
                args[0] = args[0].with_changes(value=broadcast_call)
                self.changed = True
                return updated_node.with_changes(args=args)
        return updated_node


class RepartitionBeforeGroupByTransformer(cst.CSTTransformer):
    """
    Replace patterns like:
      df = df.groupBy('k').agg(...)
    with:
      df = df.repartition(N).groupBy('k').agg(...)
    Conservative: only when left-hand side and call target are same Name.
    """
    def __init__(self, repartition_count: int = 200):
        self.repartition_count = repartition_count
        self.changed = False

    def leave_Assign(self, original_node: cst.Assign, updated_node: cst.Assign) -> cst.Assign:
        # match single-target assignment: Name = <expr>.groupBy(...)
        if len(original_node.targets) != 1:
            return updated_node
        target = original_node.targets[0].target
        if not m.matches(target, m.Name()):
            return updated_node
        target_name = target.value if isinstance(target, cst.Name) else None

        value = original_node.value
        # Want a call chain like <df>.groupBy(...)
        if m.matches(value, m.Call(func=m.Attribute(attr=m.Name("groupBy")))):
            # Extract attr chain left-most expression
            # We'll insert repartition before groupBy by rewriting the call chain
            call_node: cst.Call = value  # type: ignore
            func_attr = call_node.func
            if isinstance(func_attr, cst.Attribute):
                owner = func_attr.value
                # Check owner is the same name as target (common pattern df = df.groupBy(...))
                if m.matches(owner, m.Name(value=target_name)):
                    # create new expression: <owner>.repartition(N).groupBy(...)
                    repart_call = cst.Attribute(value=owner, attr=cst.Name("repartition"))
                    # build call: <owner>.repartition(N)
                    repart_call_full = cst.Call(func=repart_call, args=[cst.Arg(cst.Integer(str(self.repartition_count)))])
                    # then .groupBy(...) call on that
                    new_group_attr = cst.Attribute(value=repart_call_full, attr=cst.Name("groupBy"))
                    new_call = call_node.with_changes(func=new_group_attr)
                    self.changed = True
                    return updated_node.with_changes(value=new_call)
        return updated_node


class InsertRenameAfterReadTransformer(cst.CSTTransformer):
    """
    After a statement of the form:
      df = spark.read.<format>(...)
    Insert a following statement:
      df = df.withColumnRenamed('old','new')
    Only applies if the rename pair is requested and no existing rename is found.
    """
    def __init__(self, old_col: Optional[str], new_col: Optional[str]):
        self.old_col = old_col
        self.new_col = new_col
        self.changed = False
        # track names for which we inserted rename to avoid duplicates in same module
        self.inserted_for = set()

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        # We'll reconstruct module with additional statements inserted
        new_body = []
        body = list(updated_node.body)
        i = 0
        while i < len(body):
            stmt = body[i]
            new_body.append(stmt)
            # check if stmt is an assignment like df = spark.read...(...)
            if self.old_col and self.new_col and m.matches(stmt, m.SimpleStatementLine(body=[m.Assign(targets=[m.AssignTarget(target=m.Name())], value=m.Call(func=m.Attribute(value=m.Attribute(value=m.Name("spark"), attr=m.Name("read")))))])):
                # extract df var name
                assign = stmt.body[0]  # type: ignore
                df_name = assign.targets[0].target.value  # type: ignore
                # check next statements to avoid duplicate rename
                already = False
                # scan next up to 3 statements
                for lookahead in body[i+1:i+4]:
                    if m.matches(lookahead, m.SimpleStatementLine(body=[m.Assign(targets=[m.AssignTarget(target=m.Name(value=df_name))], value=m.Call(func=m.Attribute(value=m.Name(df_name), attr=m.Name("withColumnRenamed"))))])):
                        already = True
                        break
                if not already and df_name not in self.inserted_for:
                    # create new stmt: df = df.withColumnRenamed('old','new')
                    rename_call = cst.Call(func=cst.Attribute(value=cst.Name(df_name), attr=cst.Name("withColumnRenamed")),
                                           args=[cst.Arg(cst.SimpleString(f"'{self.old_col}'")), cst.Arg(cst.SimpleString(f"'{self.new_col}'"))])
                    assign_stmt = cst.SimpleStatementLine([cst.Assign(targets=[cst.AssignTarget(target=cst.Name(df_name))], value=rename_call)])
                    new_body.append(assign_stmt)
                    self.inserted_for.add(df_name)
                    self.changed = True
            i += 1
        return updated_node.with_changes(body=new_body)


class ImportInserter(cst.CSTTransformer):
    """
    Ensure `from pyspark.sql.functions import broadcast` exists.
    If not present, insert it among imports near top.
    """
    def __init__(self, ensure_broadcast: bool = True):
        self.ensure_broadcast = ensure_broadcast
        self.inserted = False

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        if not self.ensure_broadcast:
            return updated_node
        # Check existing imports
        has_broadcast = False
        for stmt in updated_node.body:
            if m.matches(stmt, m.SimpleStatementLine(body=[m.FromImport(module=m.Attribute(value=m.Name("pyspark"), attr=m.Name("sql")), names=[m.ImportAlias(name=m.Name("functions"))])])):
                # This matches `from pyspark.sql import functions` not exact; we check names later.
                pass
            if m.matches(stmt, m.SimpleStatementLine(body=[m.FromImport(module=m.Name("pyspark.sql.functions"), names=[m.ImportAlias(name=m.Name("broadcast"))])])):
                has_broadcast = True
                break
            # also check `from pyspark.sql.functions import broadcast` as Name token above
            if m.matches(stmt, m.SimpleStatementLine(body=[m.FromImport(module=m.Attribute(value=m.Attribute(value=m.Name("pyspark"), attr=m.Name("sql")), attr=m.Name("functions")), names=[m.ImportAlias(name=m.Name("broadcast"))])])):
                has_broadcast = True
                break
            # generic check strings
            try:
                code = stmt.code
                if "from pyspark.sql.functions import broadcast" in code:
                    has_broadcast = True
                    break
            except Exception:
                pass

        if has_broadcast:
            return updated_node

        # Insert import after initial docstring or initial imports
        new_body = list(updated_node.body)
        insert_idx = 0
        # Skip module docstring (if present)
        if new_body and isinstance(new_body[0], cst.SimpleStatementLine) and new_body[0].body and isinstance(new_body[0].body[0], cst.Expr) and isinstance(new_body[0].body[0].value, cst.SimpleString):
            insert_idx = 1
        # Insert import
        import_stmt = cst.SimpleStatementLine([cst.FromImport(module=cst.Attribute(value=cst.Attribute(value=cst.Name("pyspark"), attr=cst.Name("sql")), attr=cst.Name("functions")), names=[cst.ImportAlias(name=cst.Name("broadcast"))])])
        new_body.insert(insert_idx, import_stmt)
        self.inserted = True
        return updated_node.with_changes(body=new_body)


# -----------------------
# Orchestration
# -----------------------

def apply_transforms(source: str,
                     do_broadcast: bool = False,
                     do_repartition: Optional[int] = None,
                     rename_pair: Optional[Tuple[str,str]] = None) -> Tuple[str, List[str]]:
    """
    Apply requested transforms and return (modified_source, list_of_actions).
    """
    module = cst.parse_module(source)
    actions = []

    # 1) Insert rename after spark.read if requested
    if rename_pair is not None:
        old_col, new_col = rename_pair
        t = InsertRenameAfterReadTransformer(old_col, new_col)
        module = module.visit(t)
        if t.changed:
            actions.append(f"rename_{old_col}_to_{new_col}")

    # 2) Repartition before groupBy
    if do_repartition:
        t2 = RepartitionBeforeGroupByTransformer(repartition_count=do_repartition)
        module = module.visit(t2)
        if t2.changed:
            actions.append(f"repartition_before_groupby_{do_repartition}")

    # 3) Broadcast join wrapping
    if do_broadcast:
        t3 = BroadcastJoinTransformer()
        module = module.visit(t3)
        if t3.changed:
            actions.append("broadcast_wrap_join")
        # Ensure import exists if changed
        if t3.changed:
            inserter = ImportInserter(ensure_broadcast=True)
            module = module.visit(inserter)
            if inserter.inserted:
                actions.append("insert_broadcast_import")

    new_src = module.code
    return new_src, actions


def cli_main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-file", required=True, help="Path to .py job file to patch")
    parser.add_argument("--broadcast", action="store_true", help="Attempt to wrap simple joins with broadcast()")
    parser.add_argument("--repartition", type=int, default=None, help="Insert repartition(N) before groupBy")
    parser.add_argument("--rename", type=str, default=None, help="Rename column in format old:new, inserted after spark.read assignment")
    args = parser.parse_args()

    job_file = args.job_file
    if not os.path.exists(job_file):
        print(json.dumps({"error": f"job file not found: {job_file}"}))
        return

    source = read_file(job_file)
    rename_pair = None
    if args.rename:
        try:
            old, new = args.rename.split(":", 1)
            rename_pair = (old.strip(), new.strip())
        except Exception:
            print(json.dumps({"error": "invalid rename format; use old:new"}))
            return

    new_src, actions = apply_transforms(source, do_broadcast=args.broadcast, do_repartition=args.repartition, rename_pair=rename_pair)

    if new_src == source:
        out = {"applied": False, "actions": actions, "message": "No transforms applied"}
        print(json.dumps(out, indent=2))
        return

    patched_path = job_file + ".patched"
    patch_path = job_file + ".patch"
    write_file(patched_path, new_src)
    patch_text = unified_diff(source, new_src, job_file, patched_path)
    write_file(patch_path, patch_text)
    out = {
        "applied": True,
        "actions": actions,
        "patched_path": patched_path,
        "patch_path": patch_path,
        "confidence": 0.85  # heuristic baseline; combine with RCA in orchestrator
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    cli_main()
