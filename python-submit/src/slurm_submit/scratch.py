"""Scratch directory setup and cleanup emission for sbatch scripts."""

from __future__ import annotations

import io


def emit_scratch_setup(out: io.StringIO, scratch_base: str, array_mode: bool) -> None:
    """Emit scratch directory setup lines for sbatch script.

    Args:
        out: Output buffer.
        scratch_base: Base scratch directory path.
        array_mode: Whether this is an array job.
    """
    assert isinstance(out, io.StringIO), "out must be an io.StringIO"
    assert isinstance(scratch_base, str), "scratch_base must be a string"

    if array_mode:
        out.write(
            f'scratch_directory="{scratch_base}/$SLURM_JOB_ID/$SLURM_ARRAY_TASK_ID"\n'
        )
    else:
        out.write(f'scratch_directory="{scratch_base}/$SLURM_JOB_ID"\n')
    out.write('mkdir -p "$scratch_directory"\n')


def emit_scratch_cleanup(out: io.StringIO) -> None:
    """Emit scratch directory cleanup lines for sbatch script.

    Args:
        out: Output buffer.
    """
    assert isinstance(out, io.StringIO), "out must be an io.StringIO"

    out.write('rm -rf "$scratch_directory"\n')
