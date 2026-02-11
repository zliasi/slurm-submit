"""Entry point and main flow for slurm-submit.

Resolves module from invocation name, loads config, parses args,
generates sbatch script, and submits to SLURM.
"""

from __future__ import annotations

import logging
import os
import sys

from slurm_submit.args import (
    _EXPORT_SENTINEL,
    ParsedArgs,
    parse_common_args,
    print_common_options,
    validate_common_args,
)
from slurm_submit.backup import backup_existing_file
from slurm_submit.config import (
    RuntimeConfig,
    apply_module_defaults,
    find_config_dir,
    init_runtime_config,
    load_defaults,
    load_software_config,
)
from slurm_submit.core import (
    SubmitError,
    UsageError,
    ensure_directory,
    normalize_output_dir,
    program_invocation,
)
from slurm_submit.manifest import create_manifest, resolve_inputs
from slurm_submit.module_base import ModuleMetadata, ScriptContext, SubmitModule
from slurm_submit.modules import MODULE_REGISTRY
from slurm_submit.partition import setup_partition_specifics
from slurm_submit.sbatch import submit_job

logger = logging.getLogger("slurm_submit")


def resolve_module(argv: list[str]) -> tuple[str, str, list[str]]:
    """Resolve which module to use from invocation name or first arg.

    Args:
        argv: sys.argv (program name + arguments).

    Returns:
        Tuple of (module_name, program_invocation, remaining_args).
    """
    assert isinstance(argv, list), "argv must be a list"
    assert len(argv) >= 1, "argv must contain at least the program name"

    invocation = os.path.basename(argv[0])

    if invocation == "submit":
        if len(argv) < 2:
            logger.error("Usage: submit <module> [input...] [options]")
            logger.error("       s<module> [input...] [options]")
            sys.exit(1)
        module_name = argv[1]
        remaining = argv[2:]
    else:
        module_name = (
            invocation.lstrip("s") if invocation.startswith("s") else invocation
        )
        remaining = argv[1:]

    if module_name not in MODULE_REGISTRY:
        logger.error("Unknown module: %s", module_name)
        sys.exit(1)

    return module_name, invocation, remaining


def print_usage(invocation: str, module: SubmitModule) -> None:
    """Print full usage: common options + module-specific help.

    Args:
        invocation: Program invocation name.
        module: Module instance.
    """
    logger.info("Usage: %s [input...] [options]\n", invocation)
    module.print_usage()
    logger.info("")
    print_common_options()


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 success, 1 error).
    """
    try:
        return _main_inner()
    except UsageError as exc:
        logger.error("Error: %s", exc)
        logger.error("Use: %s -h for help.\n", program_invocation())
        return 1
    except SubmitError as exc:
        logger.error("Error: %s", exc)
        return 1


def _parse_and_validate(
    module: SubmitModule,
    meta: ModuleMetadata,
    config: RuntimeConfig,
    invocation: str,
    argv: list[str],
) -> ParsedArgs:
    """Parse CLI args and validate common arguments.

    Args:
        module: Resolved submit module.
        meta: Module metadata.
        config: RuntimeConfig instance.
        invocation: Program invocation name.
        argv: Remaining CLI arguments.

    Returns:
        Parsed arguments object.
    """
    usage_fn = lambda: print_usage(invocation, module)  # noqa: E731
    parsed = parse_common_args(argv, config, print_usage_fn=usage_fn)
    validate_common_args(config, meta.memory_unit)
    config.output_dir = normalize_output_dir(config.output_dir)
    module.parse_args(parsed.remaining_args, config)
    return parsed


def _resolve_job_inputs(
    module: SubmitModule,
    meta: ModuleMetadata,
    config: RuntimeConfig,
    parsed: ParsedArgs,
) -> tuple[list[str], bool]:
    """Resolve inputs and set array mode on config.

    Args:
        module: Resolved submit module.
        meta: Module metadata.
        config: RuntimeConfig instance.
        parsed: Parsed arguments object.

    Returns:
        Tuple of (inputs, array_mode).
    """
    if hasattr(module, "set_inputs"):
        if module.has_custom_build_jobs:
            inputs, array_mode = module.build_jobs(
                parsed.positional_args,
                config,
            )
        else:
            inputs, array_mode = resolve_inputs(
                config,
                parsed.positional_args,
                meta.input_extensions,
            )
        module.set_inputs(inputs)
        module.validate(config)
        config.array_mode = array_mode
        setup_partition_specifics(config)
    else:
        module.validate(config)
        setup_partition_specifics(config)
        if module.has_custom_build_jobs:
            inputs, array_mode = module.build_jobs(
                parsed.positional_args,
                config,
            )
        else:
            inputs, array_mode = resolve_inputs(
                config,
                parsed.positional_args,
                meta.input_extensions,
            )
        config.array_mode = array_mode

    assert len(inputs) >= 1, "must resolve at least one input"
    return inputs, array_mode


def _determine_job_name_and_manifest(
    module: SubmitModule,
    meta: ModuleMetadata,
    config: RuntimeConfig,
    inputs: list[str],
) -> tuple[str, str]:
    """Determine job name and create manifest for array jobs.

    Args:
        module: Resolved submit module.
        meta: Module metadata.
        config: RuntimeConfig instance.
        inputs: Resolved input file paths.

    Returns:
        Tuple of (job_name, exec_manifest).
    """
    if config.custom_job_name:
        job_name = config.custom_job_name
    elif module.has_custom_determine_job_name:
        job_name = module.determine_job_name(config)
    elif config.array_mode:
        job_name = f"{meta.name}-array-{len(inputs)}t{config.throttle}"
    else:
        job_name = module.job_name(inputs[0])

    exec_manifest = ""
    if config.array_mode:
        if module.has_custom_create_exec_manifest:
            exec_manifest = module.create_exec_manifest(job_name)
            logger.info("Created manifest file: %s", exec_manifest)
        elif config.manifest_file:
            exec_manifest = config.manifest_file
            logger.info("Using manifest file: %s", exec_manifest)
        else:
            exec_manifest = create_manifest(
                inputs,
                job_name,
                config.custom_job_name,
            )
            logger.info("Created manifest file: %s", exec_manifest)

    assert isinstance(job_name, str), "job_name must be a string"
    return job_name, exec_manifest


def _backup_outputs(
    module: SubmitModule,
    config: RuntimeConfig,
    inputs: list[str],
) -> None:
    """Backup existing output files before submission.

    Args:
        module: Resolved submit module.
        config: RuntimeConfig instance.
        inputs: Resolved input file paths.
    """
    if module.has_custom_backup_all:
        module.backup_all(config)
    else:
        for input_file in inputs:
            stem = module.job_name(input_file)
            for target in module.backup_targets(
                stem,
                config.output_dir,
                config,
            ):
                backup_existing_file(
                    target,
                    config.use_backup_dir,
                    config.backup_dir_name,
                    config.max_backups,
                )


def _main_inner() -> int:
    """Inner main logic.

    Returns:
        Exit code.
    """
    config_dir = find_config_dir()
    defaults = load_defaults(config_dir)

    module_name, invocation, argv = resolve_module(sys.argv)
    module_cls = MODULE_REGISTRY[module_name]
    module = module_cls()
    meta = module.metadata

    config = init_runtime_config(defaults)
    apply_module_defaults(
        config,
        meta.default_cpus,
        meta.default_memory_gb,
        meta.default_throttle,
        meta.default_output_dir,
    )

    parsed = _parse_and_validate(module, meta, config, invocation, argv)

    if config.export_file == _EXPORT_SENTINEL:
        config.export_file = f"{module_name}.slurm"

    software = load_software_config(config_dir, module_name, config.variant)

    inputs, _ = _resolve_job_inputs(module, meta, config, parsed)

    output_path = config.output_dir.rstrip("/") if config.output_dir != "./" else "."
    ensure_directory(output_path)

    job_name, exec_manifest = _determine_job_name_and_manifest(
        module,
        meta,
        config,
        inputs,
    )
    _backup_outputs(module, config, inputs)

    ctx = ScriptContext(
        config=config,
        software=software,
        metadata=meta,
        module=module,
        inputs=inputs,
        job_name=job_name,
        exec_manifest=exec_manifest,
        node_exclude=config.node_exclude,
    )

    return submit_job(ctx)
