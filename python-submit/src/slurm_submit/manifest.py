"""Input resolution and manifest creation/reading."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from slurm_submit.core import (
    die_usage,
    to_absolute_path,
    validate_file_exists,
    validate_file_extension,
)

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig

logger = logging.getLogger("slurm_submit")


def resolve_inputs(
    config: RuntimeConfig,
    positional_args: list[str],
    input_extensions: tuple[str, ...],
) -> tuple[list[str], bool]:
    """Resolve inputs from CLI positional args or manifest file.

    Args:
        config: RuntimeConfig (reads manifest_file).
        positional_args: Non-flag CLI arguments.
        input_extensions: Allowed input extensions.

    Returns:
        Tuple of (inputs list, array_mode flag).

    Raises:
        UsageError: If no inputs or invalid files.
    """
    assert isinstance(positional_args, list), "positional_args must be list"
    assert isinstance(input_extensions, tuple), "input_extensions must be tuple"

    if config.manifest_file:
        validate_file_exists(config.manifest_file)
        with open(config.manifest_file) as fh:
            inputs = [line.strip().rstrip("\r") for line in fh if line.strip()]
        array_mode = True
    elif len(positional_args) > 1:
        inputs = list(positional_args)
        array_mode = True
    elif len(positional_args) == 1:
        inputs = [positional_args[0]]
        array_mode = False
    else:
        die_usage("No input files specified")
        return [], False

    for input_file in inputs:
        validate_file_exists(input_file)
        if input_extensions:
            validate_file_extension(input_file, input_extensions)

    return inputs, array_mode


def create_manifest(inputs: list[str], job_name: str, custom_job_name: str) -> str:
    """Create a manifest file from inputs list.

    Args:
        inputs: List of input file paths.
        job_name: Default job name for manifest filename.
        custom_job_name: Custom job name override.

    Returns:
        Path to created manifest file.
    """
    assert isinstance(inputs, list), "inputs must be a list"
    assert len(inputs) > 0, "inputs must not be empty"

    if custom_job_name:
        manifest_path = custom_job_name
    else:
        manifest_path = f".{job_name}.manifest"

    with open(manifest_path, "w") as fh:
        for input_file in inputs:
            fh.write(to_absolute_path(input_file) + "\n")

    return manifest_path


def default_job_name(input_file: str, input_extensions: tuple[str, ...]) -> str:
    """Compute job name from single input file by stripping extension.

    Args:
        input_file: Input file path.
        input_extensions: Module's input extensions.

    Returns:
        Job name string.
    """
    assert isinstance(input_file, str), "input_file must be a string"
    base = os.path.basename(input_file)
    for ext in input_extensions:
        if base.endswith(ext):
            return base[: -len(ext)]
    return base


def default_backup_targets(
    stem: str,
    output_dir: str,
    output_extensions: tuple[str, ...],
    log_extension: str,
    archive: bool,
) -> list[str]:
    """List default backup targets for pre-submit backup.

    Args:
        stem: Input stem.
        output_dir: Output directory path.
        output_extensions: Module's output extensions.
        log_extension: Log file extension.
        archive: Whether to include archive target.

    Returns:
        List of file paths to backup.
    """
    assert isinstance(stem, str), "stem must be a string"
    assert isinstance(output_dir, str), "output_dir must be a string"
    targets = []
    for ext in output_extensions:
        targets.append(f"{output_dir}{stem}{ext}")
    targets.append(f"{output_dir}{stem}{log_extension}")
    if archive:
        targets.append(f"{output_dir}{stem}.tar.xz")
    return targets
