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


# ============================================================================
# STATIC ANALYSIS: Extract table I/O from notebook code
# ============================================================================


def extract_spark_io(notebook_code: str) -> tuple[Set[str], Set[str]]:
    """
    Extract spark.read.table() and saveAsTable() calls from notebook code.

    Returns: (reads, writes) — sets of fully-qualified table names
    """
    reads = set()
    writes = set()

    # spark.read.table("catalog.schema.table")
    read_pattern = r'spark\.read\.table\(["\']([^"\']+)["\']\)'
    reads.update(re.findall(read_pattern, notebook_code))

    # .saveAsTable("catalog.schema.table")
    write_pattern = r'saveAsTable\(["\']([^"\']+)["\']\)'
    writes.update(re.findall(write_pattern, notebook_code))

    # Also catch: df.write.format("delta").mode(...).saveAsTable(...)
    # (handled by write_pattern above)

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
            # Export the notebook as text/plain
            exported = w.workspace.export(path=notebook_path, format="SOURCE")
            notebook_content = exported.contents.decode('utf-8') if isinstance(exported.contents, bytes) else exported.contents

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


def detect_topological_errors(job, task_io: Dict[str, TableIO]) -> List[str]:
    """
    Compare job.depends_on with table I/O.

    Detect: task B reads a table that task C writes, but C comes after B in the DAG.
    This is the "11/12 bug" — a topological inversion.

    Returns: list of error strings (empty = no inversions)
    """
    errors = []
    task_order = {task.task_key: i for i, task in enumerate(job.settings.tasks)}

    # Build: which task writes each table?
    table_writer = {}
    for task_key, io in task_io.items():
        for table in io.writes:
            if table in table_writer:
                # Multiple writers for same table (warning)
                pass
            table_writer[table] = task_key

    # Check: does each task read tables written by tasks that come after it?
    for task_key, io in task_io.items():
        task_idx = task_order[task_key]

        for read_table in io.reads:
            if read_table in table_writer:
                writer_task = table_writer[read_table]
                writer_idx = task_order[writer_task]

                if writer_idx > task_idx:
                    # Task reads a table from a task that comes LATER
                    errors.append(
                        f"Topological inversion: task '{task_key}' (index {task_idx}) reads "
                        f"'{read_table}', but task '{writer_task}' (index {writer_idx}) writes it. "
                        f"Task order is inverted."
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

    # Step 2: Run the job
    print("[Full run] Step 2: Triggering job run...")
    run_response = w.jobs.run_now(job_id=job_id)
    run_id = run_response.result().run_id
    print(f"  Run ID: {run_id}")

    # Step 3: Wait for completion
    print(f"[Full run] Step 3: Waiting for completion (max {timeout_minutes} min)...")
    try:
        run = w.jobs.run_now_and_wait(
            job_id=job_id,
            timeout=timedelta(minutes=timeout_minutes)
        )
    except Exception as e:
        print(f"  ✗ Run timed out: {e}")
        return {
            "status": "TIMEOUT",
            "run_id": run_id,
            "job_id": job_id,
            "error": str(e)
        }

    # Step 4: Analyze results
    print("[Full run] Step 4: Analyzing results...")
    run = w.jobs.get_run(run_id)

    overall_result = run.state.result_state if run.state else None
    lifecycle = run.state.life_cycle_state if run.state else None

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

    # Extract common fields
    status = validation_result.get("status", "UNKNOWN")
    run_id = validation_result.get("run_id")
    errors = validation_result.get("errors", [])
    todos = validation_result.get("todos", [])

    # Record in migration_factory registry
    if status == 'VALIDATED':
        # Update object status
        w.sql.execute(
            f"""
            UPDATE {client_schema}.objects
            SET status = 'validated',
                confidence = 0.95,
                updated_at = current_timestamp()
            WHERE object_id = '{object_id}'
            """
        )
        return {
            "status": "VALIDATED",
            "object_id": object_id,
            "run_id": run_id,
            "todos_count": 0
        }

    elif status == 'BLOCKED':
        # Record blocking errors as TODOs
        for i, error in enumerate(errors):
            w.sql.execute(
                f"""
                INSERT INTO {client_schema}.todos
                (object_id, status, severity, message, action, created_at)
                VALUES ('{object_id}', 'BLOCKED', 'CRITICAL', '{error}', 'FIX_JOB_STRUCTURE', current_timestamp())
                """
            )
        return {
            "status": "BLOCKED",
            "object_id": object_id,
            "run_id": None,
            "todos_count": len(errors)
        }

    else:  # FAILED
        # Record TODOs for each failed task
        for todo in todos:
            w.sql.execute(
                f"""
                INSERT INTO {client_schema}.todos
                (object_id, task_key, status, severity, message, action, run_id, created_at)
                VALUES ('{object_id}', '{todo['task_key']}', 'TODO', '{todo['severity']}',
                        '{todo['error']}', '{todo['action']}', {run_id}, current_timestamp())
                """
            )

        # Update object with partial validation
        w.sql.execute(
            f"""
            UPDATE {client_schema}.objects
            SET status = 'needs_review',
                confidence = 0.3,
                updated_at = current_timestamp()
            WHERE object_id = '{object_id}'
            """
        )

        return {
            "status": "FAILED",
            "object_id": object_id,
            "run_id": run_id,
            "todos_count": len(todos)
        }


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
