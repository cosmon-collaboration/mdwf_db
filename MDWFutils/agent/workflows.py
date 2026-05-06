"""Deterministic workflow planners for agent-assisted lattice practice."""

from __future__ import annotations

from uuid import uuid4

from .contracts import ActionPlan, ActionStep, RiskLevel


def plan_workflow(name: str, args: dict) -> ActionPlan:
    """Expand a named workflow into exact agent-visible steps."""
    planners = {
        "curate_new_ensemble": _curate_new_ensemble,
        "prepare_measurement_job": _prepare_measurement_job,
        "monitor_jobs": _monitor_jobs,
        "ingest_measurements": _ingest_measurements,
        "export_analysis": _export_analysis,
    }
    if name not in planners:
        raise ValueError(f"Unknown workflow '{name}'")
    return planners[name](args)


def _plan_id(name: str) -> str:
    return f"{name}-{uuid4().hex[:10]}"


def _curate_new_ensemble(args: dict) -> ActionPlan:
    cli_args = dict(args)
    if "physics" in cli_args and "params" not in cli_args:
        cli_args["params"] = _physics_params_string(cli_args.pop("physics"))
    target = {
        "directory": cli_args.get("directory"),
        "nickname": cli_args.get("nickname"),
        "status": cli_args.get("status", "TUNING"),
        "physics": args.get("physics", {}),
    }
    steps = [
        ActionStep(
            id="observe_commands",
            tool="metadata.commands",
            risk=RiskLevel.READ,
            expected_effects=["Confirm available add-ensemble, scan, update, and recipe tools."],
        ),
        ActionStep(
            id="preflight_add_ensemble",
            tool="cli.add-ensemble",
            args={**cli_args, "dry_run": True, "json": True},
            risk=RiskLevel.READ,
            expected_effects=["Validate path, physics parameters, nickname, and planned directory creation."],
        ),
        ActionStep(
            id="add_ensemble",
            tool="cli.add-ensemble",
            args={**cli_args, "json": True},
            risk=RiskLevel.WRITES_FILES,
            requires_confirmation=True,
            expected_effects=["Create ensemble directory tree and MongoDB ensemble document."],
            verify_with="cli.status",
        ),
        ActionStep(
            id="scan_dry_run",
            tool="cli.scan",
            args={"ensemble": args.get("nickname") or args.get("directory"), "dry_run": True, "json": True},
            risk=RiskLevel.READ,
            expected_effects=["Report discovered configurations and pending measurement files."],
        ),
        ActionStep(
            id="scan_apply",
            tool="cli.scan",
            args={"ensemble": args.get("nickname") or args.get("directory"), "json": True},
            risk=RiskLevel.WRITES_DB,
            requires_confirmation=True,
            expected_effects=["Persist configuration list and counts."],
            verify_with="cli.status",
        ),
    ]
    return ActionPlan(
        plan_id=_plan_id("curate_new_ensemble"),
        workflow="curate_new_ensemble",
        target=target,
        steps=steps,
        assumptions=["Thermalization, tags, HMC paths, and recipes are applied as follow-up curation steps once known."],
    )


def _physics_params_string(physics: dict) -> str:
    order = ["beta", "b", "Ls", "mc", "ms", "ml", "L", "T"]
    return " ".join(f"{key}={physics[key]}" for key in order if key in physics)


def _prepare_measurement_job(args: dict) -> ActionPlan:
    ensemble = args.get("ensemble")
    measurement = args.get("measurement_type", "mres")
    script_names = {
        "mres": "mres-script",
        "mres_mq": "mres-mq-script",
        "meson2pt": "meson2pt-script",
        "wflow": "wflow-script",
        "smear": "smear-script",
        "zv": "zv-script",
    }
    script_tool = f"cli.{script_names.get(measurement, measurement)}"
    script_args = {
        key: value
        for key, value in args.items()
        if key not in ("measurement_type", "script")
    }
    return ActionPlan(
        plan_id=_plan_id("prepare_measurement_job"),
        workflow="prepare_measurement_job",
        target={"ensemble": ensemble, "measurement_type": measurement},
        steps=[
            ActionStep(
                id="read_status",
                tool="cli.status",
                args={"ensemble": ensemble, "missing": measurement, "json": True},
                risk=RiskLevel.READ,
                expected_effects=["Identify expected and missing configurations."],
            ),
            ActionStep(
                id="script_dry_run",
                tool=script_tool,
                args={**script_args, "dry_run": True, "json": True},
                risk=RiskLevel.READ,
                expected_effects=["Validate input/job params and planned script/input paths."],
            ),
            ActionStep(
                id="write_script",
                tool=script_tool,
                args={**script_args, "json": True},
                risk=RiskLevel.WRITES_FILES,
                requires_confirmation=True,
                expected_effects=["Write SLURM script and input file."],
                verify_with="cli.status",
            ),
            ActionStep(
                id="submit_script",
                tool="cli.submit",
                args={"script": args.get("script"), "ensemble": ensemble, "operation_type": measurement, "json": True},
                risk=RiskLevel.SUBMITS_JOBS,
                requires_confirmation=True,
                expected_effects=["Submit script to SLURM and create PENDING operation record."],
                verify_with="cli.status",
            ),
        ],
    )


def _monitor_jobs(args: dict) -> ActionPlan:
    return ActionPlan(
        plan_id=_plan_id("monitor_jobs"),
        workflow="monitor_jobs",
        target={"ensemble": args.get("ensemble")},
        steps=[
            ActionStep(
                id="monitor_dry_run",
                tool="cli.monitor",
                args={**args, "dry_run": True, "json": True},
                risk=RiskLevel.READ,
                expected_effects=["Compare active operation records with scheduler state."],
            ),
            ActionStep(
                id="monitor_apply",
                tool="cli.monitor",
                args={**args, "json": True},
                risk=RiskLevel.WRITES_DB,
                requires_confirmation=True,
                expected_effects=["Apply approved status reconciliations."],
                verify_with="cli.status",
            ),
        ],
    )


def _ingest_measurements(args: dict) -> ActionPlan:
    return ActionPlan(
        plan_id=_plan_id("ingest_measurements"),
        workflow="ingest_measurements",
        target={"ensemble": args.get("ensemble"), "measurement_type": args.get("measurement_type", "all")},
        steps=[
            ActionStep(
                id="scan",
                tool="cli.scan",
                args={"ensemble": args.get("ensemble"), "dry_run": True, "json": True},
                risk=RiskLevel.READ,
                expected_effects=["Discover candidate measurement files."],
            ),
            ActionStep(
                id="ingest_dry_run",
                tool="cli.ingest",
                args={**args, "dry_run": True, "json": True},
                risk=RiskLevel.READ,
                expected_effects=["Count measurements that would be ingested or skipped."],
            ),
            ActionStep(
                id="ingest_apply",
                tool="cli.ingest",
                args={**args, "json": True},
                risk=RiskLevel.WRITES_DB,
                requires_confirmation=True,
                expected_effects=["Parse and upsert measurement documents."],
                verify_with="cli.status",
            ),
        ],
    )


def _export_analysis(args: dict) -> ActionPlan:
    return ActionPlan(
        plan_id=_plan_id("export_analysis"),
        workflow="export_analysis",
        target={"ensemble": args.get("ensemble"), "measurement_type": args.get("measurement_type", "all")},
        steps=[
            ActionStep(
                id="list_fields",
                tool="cli.query_fields",
                args={"measurement_type": args.get("measurement_type", "all"), "json": True},
                risk=RiskLevel.READ,
                expected_effects=["Confirm exportable fields."],
            ),
            ActionStep(
                id="query_export",
                tool="cli.query",
                args={**args, "json": True},
                risk=RiskLevel.WRITES_FILES if args.get("output") else RiskLevel.READ,
                requires_confirmation=bool(args.get("output")),
                expected_effects=["Export selected measurements or print selected data."],
            ),
            ActionStep(
                id="record_analysis",
                tool="analysis_runs.record",
                args=args,
                risk=RiskLevel.WRITES_DB,
                requires_confirmation=True,
                expected_effects=["Store analysis provenance."],
            ),
        ],
    )
