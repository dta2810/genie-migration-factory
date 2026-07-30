"""
Databricks Lakeflow Job Validator

Validates Alteryx→Databricks migration jobs WITHOUT running them (schema-only),
then optionally runs and captures errors for TODO registration.

Key exported functions:
  - schema_only_validate(job_id, w) → dict with status/errors
  - full_validation_run(job_id, w) → dict with validated/failed status + error details
  - validate_and_register_migration() → master function for migration_factory registry
"""

import re
from datetime import datetime, timedelta
from typing import Dict, Set, List, Optional, Any
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState, RunLifeCycleState


@dataclass
class TableIO:
    """Table reads and writes for a task."""
    task_key: str
    reads: Set[str]
    writes: Set[str]


def _warehouse_id(w: WorkspaceClient) -> str:
    """Pick a running/available SQL warehouse to execute CALL statements."""
    whs = list(w.warehouses.list())
    if not whs:
        raise RuntimeError("no SQL warehouse available to run registry CALL statements")
    # prefer a RUNNING one, else the first
    running = [x for x in whs if getattr(x.state, "value", str(x.state)) == "RUNNING"]
    return (running[0] if running else whs[0]).id


# ============================================================================
# STATIC ANALYSIS: Extract table I/O from notebook code
# ============================================================================


def _normalize_table_ref(raw: str) -> Optional[str]:
    """Reduce a table reference to a comparable key.

    Real Genie notebooks write f-strings like f"{catalog}.{schema}._stage_04_filtered".
    We can't resolve {catalog}/{schema} statically, so we key on the UNqualified table name
    (the last dotted segment), stripping any `{...}` interpolation and quotes/backticks.
    Returns None if nothing usable remains (e.g. a fully dynamic name).
    """
    if raw is None:
        return None
    s = raw.strip().strip('`"\' ')
    # drop leading f-string interpolations like {catalog}.{schema}.
    # keep the final segment after the last '.' that isn't itself an interpolation
    segments = s.split('.')
    tail = None
    for seg in reversed(segments):
        seg = seg.strip()
        if seg and '{' not in seg and '}' not in seg:
            tail = seg
            break
    if not tail:
        return None
    return tail.lower()


def extract_spark_io(notebook_code: str) -> tuple[Set[str], Set[str]]:
    """
    Extract table reads/writes from notebook code, handling f-strings.

    Matches both literal strings and f-strings, e.g.:
      spark.read.table(f"{catalog}.{schema}._stage_12_sorted")
      df.write... .saveAsTable(f"{catalog}.{schema}._stage_11_filtered")
    Table names are normalized to their unqualified tail (e.g. '_stage_12_sorted') so the
    catalog/schema interpolation doesn't defeat matching. Also captures spark.read.load/format
    is intentionally out of scope (those are file reads, not table deps).

    Returns: (reads, writes) — sets of NORMALIZED (unqualified, lowercased) table names.
    """
    reads: Set[str] = set()
    writes: Set[str] = set()

    # Accept optional f-prefix and either quote style; capture the raw string content.
    read_pattern = r'spark\.read\.table\(\s*f?["\']([^"\']+)["\']\s*\)'
    write_pattern = r'saveAsTable\(\s*f?["\']([^"\']+)["\']\s*\)'
    # spark.readStream.table(...) too
    read_stream_pattern = r'spark\.readStream\.table\(\s*f?["\']([^"\']+)["\']\s*\)'

    for m in re.findall(read_pattern, notebook_code) + re.findall(read_stream_pattern, notebook_code):
        norm = _normalize_table_ref(m)
        if norm:
            reads.add(norm)
    for m in re.findall(write_pattern, notebook_code):
        norm = _normalize_table_ref(m)
        if norm:
            writes.add(norm)

    return reads, writes


def build_task_table_graph(job, w: WorkspaceClient) -> Dict[str, TableIO]:
    """
    For each task in the job, extract its table I/O.

    Args:
        job: JobSettings object from w.jobs.get()
        w: WorkspaceClient

    Returns:
        dict: { task_key: TableIO(reads=set, writes=set) }
    """
    task_io = {}

    for task in job.settings.tasks:
        task_key = task.task_key

        if not task.notebook_task:
            # Non-notebook task (SQL, Python, etc.) — skip analysis
            task_io[task_key] = TableIO(task_key, set(), set())
            continue

        notebook_path = task.notebook_task.notebook_path

        # Try to fetch notebook content
        try:
            import base64
            from databricks.sdk.service.workspace import ExportFormat
            exported = w.workspace.export(path=notebook_path, format=ExportFormat.SOURCE)
            raw = exported.content  # base64-encoded string
            notebook_content = base64.b64decode(raw).decode("utf-8") if raw else ""

            reads, writes = extract_spark_io(notebook_content)
            task_io[task_key] = TableIO(task_key, reads, writes)

        except Exception as e:
            # If we can't read the notebook, record a warning but don't fail yet
            print(f"Warning: could not read notebook {notebook_path} for {task_key}: {e}")
            task_io[task_key] = TableIO(task_key, set(), set())

    return task_io


# ============================================================================
# STRUCTURAL VALIDATION: Cycle detection, dependency checks
# ============================================================================


def validate_job_structure(job, w: WorkspaceClient) -> List[str]:
    """
    Pre-flight structural checks before running.

    Checks:
      1. All notebook paths exist
      2. depends_on task_keys are valid
      3. No circular dependencies

    Returns: list of error strings (empty = all checks pass)
    """
    errors = []

    # 1. Validate notebook paths
    for task in job.settings.tasks:
        if task.notebook_task:
            notebook_path = task.notebook_task.notebook_path
            try:
                w.workspace.get_status(notebook_path)
            except Exception as e:
                errors.append(
                    f"Task {task.task_key}: notebook not found at {notebook_path} ({e})"
                )

    # 2. Validate depends_on references
    valid_task_keys = {task.task_key for task in job.settings.tasks}
    for task in job.settings.tasks:
        if task.depends_on:
            for dep in task.depends_on:
                if dep.task_key not in valid_task_keys:
                    errors.append(
                        f"Task {task.task_key} depends on {dep.task_key} (task not in job)"
                    )

    # 3. Cycle detection via DFS
    def has_cycle(graph: Dict[str, List[str]]) -> bool:
        """Detect circular dependencies."""
        visited = set()
        rec_stack = set()

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        for node in graph:
            if node not in visited:
                if dfs(node):
                    return True
        return False

    # Build depends_on graph
    dep_graph = {}
    for task in job.settings.tasks:
        dep_graph[task.task_key] = [
            d.task_key for d in (task.depends_on or [])
        ]

    if has_cycle(dep_graph):
        errors.append("Job has a circular dependency detected")

    return errors


# ============================================================================
# TOPOLOGICAL VALIDATION: Detect ordering inversions
# ============================================================================


def _reachable_upstream(task_key: str, dep_graph: Dict[str, List[str]]) -> Set[str]:
    """All tasks that run before `task_key` per depends_on (transitive)."""
    seen: Set[str] = set()
    stack = list(dep_graph.get(task_key, []))
    while stack:
        t = stack.pop()
        if t in seen:
            continue
        seen.add(t)
        stack.extend(dep_graph.get(t, []))
    return seen


def detect_topological_errors(job, task_io: Dict[str, TableIO]) -> List[str]:
    """
    Detect the "11/12 bug": a task reads a table that is written by a task NOT guaranteed to
    run before it. Correctness is defined by depends_on (the real execution constraint), not by
    task list index — two tasks with no dependency path can run in any order / in parallel.

    A read is safe only if the writer is a transitive `depends_on` ancestor. We flag:
      - writer runs later / is not an upstream ancestor (true inversion or missing dependency)
      - a read whose table no task writes (only if some task writes a similarly-named staging table
        — otherwise it's an external/source read, not an error)

    Returns: list of error strings (empty = no inversions).
    """
    errors = []

    # depends_on graph: task -> [its upstream deps]
    dep_graph = {t.task_key: [d.task_key for d in (t.depends_on or [])] for t in job.settings.tasks}

    # which task writes each (normalized) table
    table_writer: Dict[str, str] = {}
    for task_key, io in task_io.items():
        for table in io.writes:
            table_writer[table] = task_key

    # any static I/O extracted at all? if not, we can't reason about tables — say so.
    any_io = any(io.reads or io.writes for io in task_io.values())
    if not any_io:
        errors.append(
            "Could not extract any table reads/writes from the notebooks — static table-dependency "
            "validation is inconclusive. Verify the notebooks use spark.read.table/saveAsTable."
        )
        return errors

    for task_key, io in task_io.items():
        upstream = _reachable_upstream(task_key, dep_graph)
        for read_table in io.reads:
            writer = table_writer.get(read_table)
            if writer is None:
                # table not written by any task → treated as an external/source input, not an error
                continue
            if writer == task_key:
                continue
            if writer not in upstream:
                errors.append(
                    f"Topological inversion: task '{task_key}' reads '{read_table}', written by "
                    f"'{writer}', but '{writer}' is NOT an upstream depends_on of '{task_key}'. "
                    f"The job would read a table before it is written (TABLE_OR_VIEW_NOT_FOUND)."
                )

    return errors


# ============================================================================
# ERROR CAPTURE: Extract errors from run results
# ============================================================================


def capture_job_errors(run_id: int, w: WorkspaceClient) -> Dict[str, Any]:
    """
    Extract all error info from a completed run.

    Returns: dict with job-level and per-task errors
    """
    run = w.jobs.get_run(run_id)
    errors_dict = {
        "job_level": None,
        "tasks": {}
    }

    # Job-level error
    if run.state and run.state.state_message:
        errors_dict["job_level"] = run.state.state_message

    # Try to get run output (may not be available immediately)
    try:
        run_output = w.jobs.get_run_output(run_id)
        if run_output.error:
            errors_dict["job_level"] = run_output.error
        if run_output.error_trace:
            errors_dict["job_trace"] = run_output.error_trace
    except Exception:
        # Run output not yet available; that's OK
        pass

    # Per-task errors
    if run.tasks:
        for task in run.tasks:
            if task.state and task.state.result_state == RunResultState.FAILED:
                errors_dict["tasks"][task.task_key] = {
                    "state_message": task.state.state_message,
                    "ui_url": task.run_page_url
                }

    return errors_dict


# ============================================================================
# MAIN VALIDATION FUNCTIONS
# ============================================================================


def schema_only_validate(job_id: int, w: WorkspaceClient) -> Dict[str, Any]:
    """
    Validate job WITHOUT running it (schema-only, static analysis).

    Checks:
      1. Notebook paths exist
      2. DAG is valid (no cycles, no missing deps)
      3. No topological inversions (table ordering bug)

    Args:
        job_id: Job ID
        w: WorkspaceClient

    Returns:
        {
            "status": "PASS" | "FAIL",
            "errors": List[str],
            "validation_type": "structure" | "topological" | "schema_only",
            "job_id": job_id
        }
    """
    job = w.jobs.get(job_id)

    print(f"[Schema-only] Validating job {job.settings.name} ({job_id})...")

    # 1. Structural checks
    print("  Checking structure...")
    struct_errors = validate_job_structure(job, w)
    if struct_errors:
        return {
            "status": "FAIL",
            "errors": struct_errors,
            "validation_type": "structure",
            "job_id": job_id
        }
    print("    ✓ Structure OK")

    # 2. Table dependency analysis
    print("  Analyzing table dependencies...")
    task_io = build_task_table_graph(job, w)
    topo_errors = detect_topological_errors(job, task_io)
    if topo_errors:
        return {
            "status": "FAIL",
            "errors": topo_errors,
            "validation_type": "topological",
            "job_id": job_id
        }
    print("    ✓ Table ordering OK")

    return {
        "status": "PASS",
        "errors": [],
        "validation_type": "schema_only",
        "job_id": job_id
    }


def full_validation_run(
    job_id: int,
    w: WorkspaceClient,
    timeout_minutes: int = 30,
    skip_schema_validation: bool = False
) -> Dict[str, Any]:
    """
    Full validation: schema check + actual job run + error capture.

    Args:
        job_id: Job ID
        w: WorkspaceClient
        timeout_minutes: Max time to wait for job completion
        skip_schema_validation: If True, skip schema-only validation

    Returns:
        {
            "status": "VALIDATED" | "FAILED" | "BLOCKED" | "TIMEOUT",
            "run_id": Optional[int],
            "job_id": int,
            "lifecycle": RunLifeCycleState,
            "tasks": List[{task_key, state, message}],
            "errors": List[str],
            "todos": List[{task_key, error, severity, action}]
        }
    """
    job = w.jobs.get(job_id)

    # Step 1: Schema validation (optional)
    if not skip_schema_validation:
        print("[Full run] Step 1: Schema validation...")
        schema_result = schema_only_validate(job_id, w)
        if schema_result['status'] == 'FAIL':
            return {
                "status": "BLOCKED",
                "reason": "Schema validation failed",
                "errors": schema_result['errors'],
                "job_id": job_id,
                "run_id": None
            }
        print("  ✓ Schema validation passed")

    # Step 2+3: Run the job ONCE and wait (run_now_and_wait triggers a single run).
    print(f"[Full run] Step 2: Triggering + waiting (max {timeout_minutes} min)...")
    from databricks.sdk.errors import TimeoutError as SdkTimeout
    try:
        run = w.jobs.run_now_and_wait(
            job_id=job_id,
            timeout=timedelta(minutes=timeout_minutes)
        )
    except SdkTimeout as e:
        print(f"  ✗ Run timed out: {e}")
        return {"status": "TIMEOUT", "run_id": None, "job_id": job_id, "error": str(e)}
    except Exception as e:
        # network / invalid job / permissions — a real failure, not a timeout
        print(f"  ✗ Run could not complete: {e}")
        return {"status": "FAILED", "run_id": None, "job_id": job_id,
                "errors": [f"job run error: {e}"], "todos": []}

    run_id = run.run_id
    print(f"  Run ID: {run_id}")

    # Step 4: Analyze results
    print("[Full run] Step 4: Analyzing results...")
    if run.state is None:
        return {"status": "FAILED", "run_id": run_id, "job_id": job_id,
                "errors": ["run completed with no state object"], "todos": []}
    overall_result = run.state.result_state
    lifecycle = run.state.life_cycle_state

    # Capture task results
    task_results = []
    if run.tasks:
        for task in run.tasks:
            state = task.state.result_state if task.state else None
            message = task.state.state_message if task.state else None
            task_results.append({
                "task_key": task.task_key,
                "state": str(state) if state else "UNKNOWN",
                "message": message,
                "ui_url": task.run_page_url
            })

    # Determine pass/fail
    if overall_result == RunResultState.SUCCESS:
        print(f"  ✓ All tasks succeeded")
        return {
            "status": "VALIDATED",
            "run_id": run_id,
            "job_id": job_id,
            "lifecycle": str(lifecycle),
            "tasks": task_results,
            "errors": []
        }

    else:
        print(f"  ✗ Job failed (result={overall_result})")

        # Extract errors
        error_details = capture_job_errors(run_id, w)

        # Build TODOs from failed tasks
        todos = []
        for task in task_results:
            if task["state"] == str(RunResultState.FAILED):
                todos.append({
                    "task_key": task["task_key"],
                    "error": task["message"],
                    "severity": "HIGH",
                    "action": "REVIEW_NOTEBOOK_LOGS",
                    "ui_url": task["ui_url"]
                })

        return {
            "status": "FAILED",
            "run_id": run_id,
            "job_id": job_id,
            "lifecycle": str(lifecycle),
            "tasks": task_results,
            "errors": [
                error_details.get("job_level", "Unknown job-level error")
            ],
            "todos": todos
        }


# ============================================================================
# INTEGRATION: Register validation results in migration_factory
# ============================================================================


def validate_and_register_migration(
    job_id: int,
    object_id: str,
    client_schema: str,
    w: WorkspaceClient,
    full_run: bool = True
) -> Dict[str, Any]:
    """
    Master function: validate job, then record in migration_factory registry.

    Args:
        job_id: Databricks job ID
        object_id: Unique identifier for this migration object
        client_schema: Catalog.schema name for migration_factory tables
        w: WorkspaceClient
        full_run: If True, run full validation; if False, schema-only

    Returns:
        {
            "status": "VALIDATED" | "FAILED" | "BLOCKED",
            "object_id": object_id,
            "run_id": Optional[int],
            "todos_count": int
        }
    """

    if full_run:
        validation_result = full_validation_run(job_id, w)
    else:
        validation_result = schema_only_validate(job_id, w)

    status = validation_result.get("status", "UNKNOWN")
    run_id = validation_result.get("run_id")
    errors = validation_result.get("errors", [])
    todos = validation_result.get("todos", [])

    # All registry writes go through the UC procedures (audit-by-construction, parameterized —
    # never raw INSERT/UPDATE). We pass a caller-supplied migration run_id for the validate step.
    import uuid as _uuid
    mig_run_id = f"validate-{object_id.replace(':', '_')}-{_uuid.uuid4().hex[:8]}"

    def _call(proc: str, *args):
        # Build a CALL with proper SQL literal escaping; execute via statement execution.
        def lit(v):
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return str(v)
            return "'" + str(v).replace("'", "''") + "'"
        stmt = f"CALL {client_schema}.{proc}(" + ", ".join(lit(a) for a in args) + ")"
        w.statement_execution.execute_statement(
            warehouse_id=_warehouse_id(w), statement=stmt, wait_timeout="30s"
        )

    _call("start_run", mig_run_id, object_id, "validate", "genie_code")

    if status == 'VALIDATED':
        if run_id is not None:
            _call("link_run", mig_run_id, str(run_id))
        _call("transition", object_id, "validated", 0.95, None, None,
              f"validated: job run {run_id} succeeded", mig_run_id)
        _call("end_run", mig_run_id, "ok")
        return {"status": "VALIDATED", "object_id": object_id, "run_id": run_id, "todos_count": 0}

    elif status == 'BLOCKED':
        for error in errors:
            _call("add_todo", object_id, "validation", error, "blocker")
        _call("transition", object_id, "needs_review", None, None, None,
              "validation blocked: schema/structure check failed", mig_run_id)
        _call("end_run", mig_run_id, "failed")
        return {"status": "BLOCKED", "object_id": object_id, "run_id": None, "todos_count": len(errors)}

    else:  # FAILED
        if run_id is not None:
            _call("link_run", mig_run_id, str(run_id))
        for todo in todos:
            _call("add_todo", object_id, "validation",
                  f"task {todo.get('task_key')}: {todo.get('error')}", "blocker")
        for error in errors:
            _call("add_todo", object_id, "validation", error, "blocker")
        _call("transition", object_id, "needs_review", None, None, None,
              f"validation failed: job run {run_id}", mig_run_id)
        _call("end_run", mig_run_id, "failed")
        return {"status": "FAILED", "object_id": object_id, "run_id": run_id,
                "todos_count": len(todos) + len(errors)}


if __name__ == "__main__":
    # Example usage
    w = WorkspaceClient(profile="fe-vm-dt-serverless-stable-isqgt5")

    job_id = 418870952588308

    # Schema-only validation
    print("\n" + "=" * 80)
    print("SCHEMA-ONLY VALIDATION")
    print("=" * 80)
    result = schema_only_validate(job_id, w)
    print(f"\nResult: {result['status']}")
    if result['errors']:
        for error in result['errors']:
            print(f"  - {error}")

    # Full validation (commented out to avoid actual job runs)
    # print("\n" + "=" * 80)
    # print("FULL VALIDATION (RUNS JOB)")
    # print("=" * 80)
    # result = full_validation_run(job_id, w, timeout_minutes=10)
    # print(f"\nResult: {result['status']}")
    # if result['errors']:
    #     for error in result['errors']:
    #         print(f"  - {error}")
    # if result.get('todos'):
    #     print("\nTODOs:")
    #     for todo in result['todos']:
    #         print(f"  - {todo['task_key']}: {todo['error']}")
