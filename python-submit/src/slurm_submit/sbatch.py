"""SBATCH script assembly.

Builds complete sbatch scripts from header, module emissions, and footer.
"""

from __future__ import annotations

import io
import logging
import math
import os
import subprocess

from slurm_submit.core import to_absolute_path
from slurm_submit.module_base import RunContext, ScriptContext
from slurm_submit.scratch import emit_scratch_cleanup, emit_scratch_setup

logger = logging.getLogger("slurm_submit")


def emit_sbatch_header(out: io.StringIO, ctx: ScriptContext) -> None:
    """Emit sbatch header with #SBATCH directives.

    Args:
        out: Output buffer.
        ctx: Script generation context.
    """
    assert isinstance(ctx, ScriptContext), "ctx must be a ScriptContext"
    assert isinstance(out, io.StringIO), "out must be an io.StringIO"

    config = ctx.config
    meta = ctx.metadata

    if meta.memory_unit == "gb_float":
        mem_mb = math.ceil(float(config.memory_gb) * 1024)
        memory_directive = f"{mem_mb}MB"
    else:
        memory_directive = f"{config.memory_gb}gb"

    out.write("#!/bin/bash\n")
    out.write(f"#SBATCH --job-name={ctx.job_name}\n")

    if config.array_mode:
        out.write('#SBATCH --output="/dev/null"\n')
        out.write(f"#SBATCH --array=1-{len(ctx.inputs)}%{config.throttle}\n")
    else:
        out.write(f'#SBATCH --output="{config.output_dir}%x{config.log_extension}"\n')

    out.write(f"#SBATCH --nodes={config.nodes}\n")
    out.write(f"#SBATCH --ntasks={config.ntasks}\n")
    out.write(f"#SBATCH --cpus-per-task={config.num_cpus}\n")
    out.write(f"#SBATCH --mem={memory_directive}\n")
    out.write(f"#SBATCH --partition={config.partition}\n")

    if config.time_limit:
        out.write(f"#SBATCH --time={config.time_limit}\n")
    if config.nice_factor:
        out.write(f"#SBATCH --nice={config.nice_factor}\n")
    if ctx.node_exclude:
        out.write(f"#SBATCH --exclude={ctx.node_exclude}\n")

    out.write("#SBATCH --export=NONE\n")


def emit_job_info_block(out: io.StringIO, ctx: ScriptContext, array_mode: bool) -> None:
    """Emit job info printf block for sbatch script.

    Args:
        out: Output buffer.
        ctx: Script generation context.
        array_mode: Whether this is an array job.
    """
    config = ctx.config
    meta = ctx.metadata
    time_display = config.time_limit or "default (partition max)"

    if meta.memory_unit == "gb_float":
        mem_per_cpu = config.memory_gb
    else:
        mem_per_cpu = str(int(config.memory_gb) // config.num_cpus)

    out.write('printf "Job information\\n"\n')
    out.write(f'printf "Job name:      %s\\n"   "{ctx.job_name}"\n')

    if array_mode:
        out.write(
            'printf "Job ID:        %s_%s\\n" '
            '"$SLURM_ARRAY_JOB_ID" "$SLURM_ARRAY_TASK_ID"\n'
        )
        out.write('printf "Input file:    %s\\n"   "$(basename "$input_file")"\n')
    else:
        out.write('printf "Job ID:        %s\\n"   "$SLURM_JOB_ID"\n')
        input_basename = os.path.basename(ctx.inputs[0]) if ctx.inputs else ""
        out.write(f'printf "Input file:    %s\\n"   "{input_basename}"\n')

    out.write('printf "Compute node:  %s\\n"   "$HOSTNAME"\n')
    out.write(f'printf "Partition:     %s\\n"   "{config.partition}"\n')
    out.write(f'printf "CPU cores:     %s\\n"   "{config.num_cpus}"\n')

    if meta.memory_unit == "gb_float":
        out.write(f'printf "Memory:        %s GB\\n" "{config.memory_gb}"\n')
    else:
        out.write(
            f'printf "Memory:        %s GB (%s GB per CPU core)\\n" \\\n'
            f'  "{config.memory_gb}" "{mem_per_cpu}"\n'
        )

    out.write(f'printf "Time limit:    %s\\n"   "{time_display}"\n')
    out.write('printf "Submitted by:  %s\\n"   "$USER"\n')
    out.write('printf "Submitted on:  %s\\n"   "$(date)"\n')


def emit_archive_block(out: io.StringIO, create_archive: bool) -> None:
    """Emit archive creation block for sbatch script.

    Args:
        out: Output buffer.
        create_archive: Whether archive creation is enabled.
    """
    if create_archive:
        out.write(
            'if tar -cJf "$output_directory$stem.tar.xz"'
            ' -C "$scratch_directory" .; then\n'
        )
        out.write(
            '  printf "\\nArchive \\"%s.tar.xz\\" has been created in %s\\n" \\\n'
            '    "$stem" "$output_directory"\n'
        )
        out.write("else\n")
        out.write(
            '  printf "\\nError: Failed to create archive %s.tar.xz in %s\\n" \\\n'
            '    "$stem" "$output_directory"\n'
        )
        out.write("fi\n")
    else:
        out.write('printf "\\nArchive creation disabled\\n"\n')


def emit_job_footer(out: io.StringIO, array_mode: bool) -> None:
    """Emit sacct footer for sbatch script.

    Args:
        out: Output buffer.
        array_mode: Whether this is an array job.
    """
    out.write("\n")
    out.write('printf "\\nEnd of job\\n"\n')
    out.write('printf "      Job ID   Job name     Memory   Wall time   CPU time\\n"\n')
    out.write("sleep 2\n")

    if array_mode:
        out.write(
            "/usr/bin/sacct -n \\\n"
            '  -j "${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}" \\\n'
            "  --format=JobID,JobName,MaxRSS,Elapsed,CPUTime --units=MB\n"
        )
    else:
        out.write(
            '/usr/bin/sacct -n -j "$SLURM_JOB_ID" \\\n'
            "  --format=JobID,JobName,MaxRSS,Elapsed,CPUTime --units=MB\n"
        )


def generate_default_array_body(out: io.StringIO, ctx: ScriptContext) -> None:
    """Generate default array job body.

    Args:
        out: Output buffer.
        ctx: Script generation context.
    """
    meta = ctx.metadata
    config = ctx.config
    input_ext = meta.input_extensions[0] if meta.input_extensions else ".inp"
    exec_manifest = to_absolute_path(ctx.exec_manifest)

    out.write("\n")
    out.write(f'input_file=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" "{exec_manifest}")\n')
    out.write(f'stem=$(basename "$input_file" {input_ext})\n')
    out.write("\n")
    out.write(f'exec 1>"{config.output_dir}${{stem}}{config.log_extension}" 2>&1\n')
    out.write("\n")

    emit_job_info_block(out, ctx, True)
    out.write("\n")

    if meta.uses_scratch:
        out.write(f'output_directory="{config.output_dir}"\n')
        emit_scratch_setup(out, config.scratch_base, True)
        out.write("\n")

    run_ctx = RunContext(
        out=out,
        input_ref="$input_file",
        stem_ref="$stem",
        config=config,
        software=ctx.software,
    )
    ctx.module.emit_run_command(run_ctx)
    out.write("\n")
    ctx.module.emit_retrieve_outputs(out, "$stem", config)

    if meta.uses_archive:
        out.write("\n")
        emit_archive_block(out, config.create_archive)

    if meta.uses_scratch:
        out.write("\n")
        emit_scratch_cleanup(out)

    emit_job_footer(out, True)


def generate_default_single_body(out: io.StringIO, ctx: ScriptContext) -> None:
    """Generate default single job body.

    Args:
        out: Output buffer.
        ctx: Script generation context.
    """
    meta = ctx.metadata
    config = ctx.config
    single_input = to_absolute_path(ctx.inputs[0])
    stem = ctx.module.job_name(ctx.inputs[0])

    out.write(f'\nstem="{stem}"\n')
    out.write("\n")
    emit_job_info_block(out, ctx, False)
    out.write("\n")

    if meta.uses_scratch:
        out.write(f'output_directory="{config.output_dir}"\n')
        emit_scratch_setup(out, config.scratch_base, False)
        out.write("\n")

    run_ctx = RunContext(
        out=out,
        input_ref=single_input,
        stem_ref=stem,
        config=config,
        software=ctx.software,
    )
    ctx.module.emit_run_command(run_ctx)
    out.write("\n")
    ctx.module.emit_retrieve_outputs(out, stem, config)

    if meta.uses_archive:
        out.write("\n")
        emit_archive_block(out, config.create_archive)

    if meta.uses_scratch:
        out.write("\n")
        emit_scratch_cleanup(out)

    emit_job_footer(out, False)


def generate_sbatch_script(ctx: ScriptContext) -> str:
    """Generate complete sbatch script.

    Args:
        ctx: Script generation context.

    Returns:
        Complete sbatch script as string.
    """
    assert isinstance(ctx, ScriptContext), "ctx must be a ScriptContext"

    out = io.StringIO()

    emit_sbatch_header(out, ctx)
    out.write("\nset -euo pipefail\n\n")
    ctx.module.emit_dependencies(out, ctx.software)

    if ctx.config.array_mode:
        if ctx.module.has_custom_generate_array_body:
            ctx.module.generate_array_body(out, ctx)
        else:
            generate_default_array_body(out, ctx)
    else:
        if ctx.module.has_custom_generate_single_body:
            ctx.module.generate_single_body(out, ctx)
        else:
            generate_default_single_body(out, ctx)

    return out.getvalue()


def _write_export(script: str, filepath: str) -> int:
    """Write sbatch script to file instead of submitting.

    Args:
        script: Generated sbatch script content.
        filepath: Output file path.

    Returns:
        0 on success, 1 on failure.
    """
    assert isinstance(script, str), "script must be a string"
    assert isinstance(filepath, str), "filepath must be a string"

    try:
        with open(filepath, "w") as fh:
            fh.write(script)
        os.chmod(filepath, 0o755)
        logger.info("Exported sbatch script to %s", filepath)
        return 0
    except OSError as exc:
        logger.error("Failed to write export file %s: %s", filepath, exc)
        return 1


def _submit_to_sbatch(script: str, ctx: ScriptContext) -> int:
    """Submit script to sbatch via subprocess.

    Args:
        script: Generated sbatch script content.
        ctx: Script generation context.

    Returns:
        0 on success, 1 on failure.
    """
    assert isinstance(script, str), "script must be a string"
    assert isinstance(ctx, ScriptContext), "ctx must be a ScriptContext"

    result = subprocess.run(
        ["sbatch"],
        input=script,
        text=True,
        capture_output=False,
    )

    if result.returncode != 0:
        logger.error("Job submission failed")
        return 1

    if ctx.config.array_mode:
        logger.info(
            "Job array: %d subjobs, throttled to %d concurrent",
            len(ctx.inputs),
            ctx.config.throttle,
        )

    return 0


def submit_job(ctx: ScriptContext) -> int:
    """Generate sbatch script and submit or export.

    If ctx.config.export_file is set, writes to file instead of submitting.

    Args:
        ctx: Script generation context.

    Returns:
        0 on success, 1 on failure.
    """
    assert isinstance(ctx, ScriptContext), "ctx must be a ScriptContext"

    script = generate_sbatch_script(ctx)

    if ctx.config.export_file:
        return _write_export(script, ctx.config.export_file)
    return _submit_to_sbatch(script, ctx)
