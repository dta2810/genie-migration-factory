"""
EXAMPLE: Using job_validator for Alteryx migration validation

This notebook demonstrates the complete workflow for validating
a Lakeflow Job generated from an Alteryx workflow migration.

Setup:
  1. Place job_validator.py in the same directory
  2. Authenticate to Databricks (default profile or specify)
  3. Have a job_id ready (e.g., from the migrate-convert skill)
"""

from databricks.sdk import WorkspaceClient
from job_validator import (
    schema_only_validate,
    full_validation_run,
    validate_and_register_migration
)


# ============================================================================
# SCENARIO 1: Quick Schema Validation (No Run)
# ============================================================================

def example_schema_only():
    """
    Fast pre-flight check: verify job structure without running.

    Use case: Skill wants to fail fast on structural issues before running.
    Cost: ~0 compute, ~2 seconds
    """
    print("\n" + "=" * 80)
    print("SCENARIO 1: Schema-Only Validation")
    print("=" * 80)

    w = WorkspaceClient(profile="fe-vm-dt-serverless-stable-isqgt5")
    job_id = 418870952588308

    # Run validation
    result = schema_only_validate(job_id, w)

    # Display results
    print(f"\nStatus: {result['status']}")
    print(f"Validation Type: {result['validation_type']}")

    if result['status'] == 'PASS':
        print("\n✓ All structural checks passed!")
        print("  - Notebook paths are valid")
        print("  - DAG has no cycles")
        print("  - No topological inversions detected")
        print("\n→ Ready for full validation run")
        return True
    else:
        print("\n✗ Validation failed:")
        for i, error in enumerate(result['errors'], 1):
            print(f"  {i}. {error}")
        print("\n→ Fix issues before running")
        return False


# ============================================================================
# SCENARIO 2: Full Validation (Schema + Run)
# ============================================================================

def example_full_validation():
    """
    Complete validation: check structure, then run job, capture results.

    Use case: Before marking migration as "validated" and deploying.
    Cost: ~$0.01–$0.10, ~1–30 minutes
    """
    print("\n" + "=" * 80)
    print("SCENARIO 2: Full Validation (Schema + Run)")
    print("=" * 80)

    w = WorkspaceClient(profile="fe-vm-dt-serverless-stable-isqgt5")
    job_id = 418870952588308

    # Run full validation
    print("\nStarting full validation...")
    result = full_validation_run(job_id, w, timeout_minutes=30)

    # Display results
    print(f"\n{'=' * 80}")
    print(f"RESULT: {result['status']}")
    print(f"{'=' * 80}")

    if result['status'] == 'VALIDATED':
        print(f"\n✓ Migration VALIDATED")
        print(f"  Run ID: {result['run_id']}")
        print(f"  Lifecycle: {result['lifecycle']}")
        print(f"  Tasks: {len(result['tasks'])} completed successfully")

        for task in result['tasks']:
            print(f"    - {task['task_key']}: {task['state']}")

        print("\n→ Ready to mark as VALIDATED in registry")
        return result

    elif result['status'] == 'FAILED':
        print(f"\n✗ Migration FAILED")
        print(f"  Run ID: {result['run_id']}")
        print(f"  Lifecycle: {result['lifecycle']}")

        if result['errors']:
            print("\nJob-level errors:")
            for error in result['errors']:
                print(f"  - {error}")

        if result['todos']:
            print("\nTasks that failed (TODOs):")
            for todo in result['todos']:
                print(f"  - {todo['task_key']}")
                print(f"    Error: {todo['error']}")
                print(f"    Action: {todo['action']}")
                print(f"    UI: {todo['ui_url']}")

        print("\n→ Review logs, fix notebooks, and retry")
        return result

    elif result['status'] == 'BLOCKED':
        print(f"\n✗ Validation BLOCKED (schema errors)")
        print("\nErrors:")
        for error in result['errors']:
            print(f"  - {error}")
        print("\n→ Fix job structure before running")
        return result

    elif result['status'] == 'TIMEOUT':
        print(f"\n✗ Validation TIMEOUT")
        print(f"  Run ID: {result['run_id']}")
        print(f"  Error: {result['error']}")
        print("\n→ Job exceeded timeout; check for hanging tasks")
        return result


# ============================================================================
# SCENARIO 3: Integration with Migration Factory
# ============================================================================

def example_registry_integration():
    """
    Full pipeline: validate + register in migration_factory UC tables.

    Use case: Skill workflow calls this to register validated/failed objects.
    Cost: validation + ~1–2 SQL INSERT operations
    """
    print("\n" + "=" * 80)
    print("SCENARIO 3: Registry Integration")
    print("=" * 80)

    w = WorkspaceClient(profile="fe-vm-dt-serverless-stable-isqgt5")

    # Parameters (would come from skill context)
    job_id = 418870952588308
    object_id = "alteryx__sample_sales_analytics_complex__v1"
    client_schema = "migration_factory.acme"  # Example

    print(f"\nValidating and registering:")
    print(f"  Object ID: {object_id}")
    print(f"  Job ID: {job_id}")
    print(f"  Registry: {client_schema}")

    # Full validation with registration
    try:
        result = validate_and_register_migration(
            job_id=job_id,
            object_id=object_id,
            client_schema=client_schema,
            w=w,
            full_run=True  # Set to False for schema-only
        )

        print(f"\n{'=' * 80}")
        print(f"REGISTERED: {result['status']}")
        print(f"{'=' * 80}")

        if result['status'] == 'VALIDATED':
            print(f"\n✓ Object marked as VALIDATED")
            print(f"  Registry updated: objects.status = 'validated'")
            print(f"  Run ID: {result['run_id']}")

        elif result['status'] == 'FAILED':
            print(f"\n✗ Object marked as NEEDS_REVIEW")
            print(f"  TODOs recorded: {result['todos_count']}")
            print(f"  Run ID: {result['run_id']}")
            print(f"\n  User action: Review TODOs in migration_factory.{client_schema}.todos")

        elif result['status'] == 'BLOCKED':
            print(f"\n✗ Object marked as BLOCKED")
            print(f"  TODOs recorded: {result['todos_count']}")
            print(f"\n  User action: Fix job structure and resubmit")

        return result

    except Exception as e:
        print(f"\n✗ Registration failed: {e}")
        raise


# ============================================================================
# SCENARIO 4: Subset Testing (Optional)
# ============================================================================

def example_subset_testing():
    """
    Test only a subset of tasks (faster feedback loop).

    Use case: Debugging a specific task without running the full DAG.
    Note: Requires job to have explicit subset support.
    """
    print("\n" + "=" * 80)
    print("SCENARIO 4: Subset Testing (Optional)")
    print("=" * 80)

    w = WorkspaceClient(profile="fe-vm-dt-serverless-stable-isqgt5")
    job_id = 418870952588308

    # Get job to see task names
    job = w.jobs.get(job_id)
    task_keys = [task.task_key for task in job.settings.tasks]

    print(f"\nJob has {len(task_keys)} tasks:")
    for i, key in enumerate(task_keys, 1):
        print(f"  {i}. {key}")

    # Example: run only the first 3 tasks
    subset = task_keys[:3]
    print(f"\nRunning subset: {subset}")

    run = w.jobs.run_now(job_id=job_id, only=subset)
    run_id = run.result().run_id

    print(f"  Run ID: {run_id}")
    print("  (In production, would poll for completion)")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import sys

    scenario = sys.argv[1] if len(sys.argv) > 1 else "1"

    if scenario == "1":
        example_schema_only()
    elif scenario == "2":
        example_full_validation()
    elif scenario == "3":
        example_registry_integration()
    elif scenario == "4":
        example_subset_testing()
    else:
        print("Usage: python EXAMPLE_USAGE.py [1|2|3|4]")
        print("  1 = Schema-only validation")
        print("  2 = Full validation (schema + run)")
        print("  3 = Registry integration")
        print("  4 = Subset testing (optional)")
