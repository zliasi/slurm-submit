"""Module: Turbomole.

Category C: multi-file (control/coord pairs), runtime backup.
"""

from __future__ import annotations

import io
import logging
import os
from typing import TYPE_CHECKING

from slurm_submit.backup import backup_existing_file, emit_backup_function_inline
from slurm_submit.core import (
    die_usage,
    to_absolute_path,
    validate_file_exists,
)
from slurm_submit.module_base import (
    ModuleMetadata,
    RunContext,
    ScriptContext,
    SubmitModule,
)
from slurm_submit.modules import register_module
from slurm_submit.sbatch import emit_job_footer

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="turbomole",
    output_extensions=(".out",),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=10,
    default_output_dir="output",
    uses_scratch=False,
    uses_archive=False,
    memory_unit="gb",
)


@register_module("turbomole")
class TurbomoleModule(SubmitModule):
    """Turbomole submission module."""

    def __init__(self) -> None:
        """Initialize module state."""
        self._jobs: list[str] = []

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " Turbomole submission (control/coord pairs)\n"
            "\n"
            " Usage:\n"
            "   sturbomole control coord [control2 coord2 ...] [options]\n"
            "   sturbomole -M FILE [options]\n"
            "\n"
            " Examples:\n"
            "   sturbomole dft_opt/control dft_opt/coord -c 4 -m 8\n"
            "   sturbomole opt1/control opt1/coord opt2/control opt2/coord -T 5\n"
            "   sturbomole -M manifest.txt -c 2 -m 4"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Reject unknown args.

        Args:
            args: Remaining args.
            config: Runtime config.
        """
        if args:
            die_usage(f"Unknown option: {args[0]}")

    def validate(self, config: RuntimeConfig) -> None:
        """No extra validation.

        Args:
            config: Runtime config.
        """

    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """Emit environment setup.

        Args:
            out: Output buffer.
            software: Software config.
        """
        if software.dependencies:
            out.write(software.dependencies + "\n")

    def emit_run_command(self, ctx: RunContext) -> None:
        """No-op for turbomole.

        Args:
            ctx: Run context.
        """

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """No-op for turbomole.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """

    def job_name(self, input_file: str) -> str:
        """Strip extension from basename.

        Args:
            input_file: Input file path.

        Returns:
            Job name.
        """
        base = os.path.basename(input_file)
        root, _ = os.path.splitext(base)
        return root

    def backup_targets(
        self, stem: str, output_dir: str, config: RuntimeConfig
    ) -> list[str]:
        """List backup targets.

        Args:
            stem: Input stem.
            output_dir: Output directory.
            config: Runtime config.

        Returns:
            File paths.
        """
        return [
            f"{output_dir}{stem}.out",
            f"{output_dir}{stem}{config.log_extension}",
        ]

    def build_jobs(
        self, positional_args: list[str], config: RuntimeConfig
    ) -> tuple[list[str], bool]:
        """Build jobs from control/coord pairs or manifest.

        Args:
            positional_args: Positional CLI args.
            config: Runtime config.

        Returns:
            Tuple of (jobs list, array_mode flag).
        """
        assert isinstance(positional_args, list), "positional_args must be a list"
        self._jobs = []

        if config.manifest_file:
            self._read_manifest(config.manifest_file)
        else:
            self._build_from_tokens(positional_args)

        if len(self._jobs) < 1:
            die_usage("No jobs assembled (check inputs)")

        assert len(self._jobs) >= 1, "jobs must not be empty after assembly"
        array_mode = len(self._jobs) > 1
        return list(self._jobs), array_mode

    def create_exec_manifest(self, job_name: str) -> str:
        """Write tab-separated manifest from jobs.

        Args:
            job_name: Job name for filename.

        Returns:
            Manifest file path.
        """
        assert job_name, "job_name must be non-empty"
        assert self._jobs, "jobs must be non-empty before manifest creation"
        manifest_path = f".{job_name}.manifest"
        with open(manifest_path, "w") as fh:
            for job in self._jobs:
                fh.write(job + "\n")
        return manifest_path

    def determine_job_name(self, config: RuntimeConfig) -> str:
        """Compute job name from jobs.

        Args:
            config: Runtime config.

        Returns:
            Job name string.
        """
        assert self._jobs, "jobs must be non-empty"
        if config.array_mode:
            return f"turbomole-array-{len(self._jobs)}t{config.throttle}"
        control, _ = self._parse_job_line(self._jobs[0])
        control_base = os.path.basename(control)
        root, _ = os.path.splitext(control_base)
        return root

    def backup_all(self, config: RuntimeConfig) -> None:
        """Backup all outputs for jobs.

        Args:
            config: Runtime config.
        """
        assert self._jobs, "jobs must be non-empty"
        for line in self._jobs:
            control, _ = self._parse_job_line(line)
            stem = os.path.splitext(os.path.basename(control))[0]
            backup_existing_file(
                f"{config.output_dir}{stem}.out",
                config.use_backup_dir,
                config.backup_dir_name,
                config.max_backups,
            )
            backup_existing_file(
                f"{config.output_dir}{stem}{config.log_extension}",
                config.use_backup_dir,
                config.backup_dir_name,
                config.max_backups,
            )

    def generate_array_body(self, out: io.StringIO, ctx: ScriptContext) -> None:
        """Generate array job body for turbomole.

        Args:
            out: Output buffer.
            ctx: Script context.
        """
        assert ctx.exec_manifest, "exec_manifest must be set for array mode"
        assert ctx.config.partition, "partition must be set"
        config = ctx.config
        exec_manifest = to_absolute_path(ctx.exec_manifest)
        mem_per_cpu = str(int(config.memory_gb) // config.num_cpus)

        out.write("\n")
        emit_backup_function_inline(
            out, config.use_backup_dir, config.backup_dir_name, config.max_backups
        )

        out.write(f'\nline=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" "{exec_manifest}")\n')
        out.write("IFS=$'\\t' read -r CONTROL COORD <<< \"$line\"\n")
        out.write("\n")
        out.write('control_dir=$(dirname "$CONTROL")\n')
        out.write('control_base=$(basename "$CONTROL")\n')
        out.write('stem="${control_base%.*}"\n')
        out.write("\n")
        out.write(f'output_file="{config.output_dir}${{stem}}.out"\n')
        out.write(f'log_file="{config.output_dir}${{stem}}{config.log_extension}"\n')
        out.write("\n")
        out.write('exec 1>"$log_file" 2>&1\n')

        self._emit_turbomole_info_block(out, ctx, mem_per_cpu, array_mode=True)

        out.write('backup_existing_files "$output_file"\n')
        out.write("\n")
        self._emit_turbomole_run_command(out, config, array_mode=True)

        emit_job_footer(out, True)
        out.write("\nexit $EXIT_CODE\n")

    def generate_single_body(self, out: io.StringIO, ctx: ScriptContext) -> None:
        """Generate single job body for turbomole.

        Args:
            out: Output buffer.
            ctx: Script context.
        """
        assert self._jobs, "jobs must be non-empty"
        assert ctx.config.partition, "partition must be set"
        config = ctx.config
        control, _ = self._parse_job_line(self._jobs[0])
        control_base = os.path.basename(control)
        stem = os.path.splitext(control_base)[0]
        mem_per_cpu = str(int(config.memory_gb) // config.num_cpus)

        out.write("\n")
        emit_backup_function_inline(
            out, config.use_backup_dir, config.backup_dir_name, config.max_backups
        )

        out.write(f'\ncontrol_dir=$(dirname "{control}")\n')
        out.write(f'stem="{stem}"\n')
        out.write("\n")
        out.write(f'output_file="{config.output_dir}${{stem}}.out"\n')

        self._emit_turbomole_info_block(out, ctx, mem_per_cpu, array_mode=False)

        out.write('backup_existing_files "$output_file"\n')
        out.write("\n")
        self._emit_turbomole_run_command(out, config, array_mode=False)

        emit_job_footer(out, False)
        out.write("\nexit $EXIT_CODE\n")

    def _emit_turbomole_info_block(
        self,
        out: io.StringIO,
        ctx: ScriptContext,
        mem_per_cpu: str,
        *,
        array_mode: bool,
    ) -> None:
        """Emit the job information printf block.

        Args:
            out: Output buffer.
            ctx: Script context.
            mem_per_cpu: Memory per CPU string.
            array_mode: Whether generating for array mode.
        """
        config = ctx.config
        time_display = config.time_limit or "default (partition max)"
        out.write("\n")
        out.write('printf "Job information\\n"\n')
        if array_mode:
            out.write(f'printf "Job name:      %s\\n"   "{ctx.job_name}"\n')
            out.write(
                'printf "Job ID:        %s_%s\\n"'
                ' "$SLURM_ARRAY_JOB_ID" "$SLURM_ARRAY_TASK_ID"\n'
            )
        else:
            out.write(
                f'printf "Job name:      %s\\n"'
                f'   "${{SLURM_JOB_NAME:-{ctx.job_name}}}"\n'
            )
            out.write('printf "Job ID:        %s\\n"   "${SLURM_JOB_ID:-}"\n')
        out.write('printf "Output file:   %s\\n"   "$output_file"\n')
        out.write('printf "Compute node:  %s\\n"   "$(hostname)"\n')
        out.write(f'printf "Partition:     %s\\n"   "{config.partition}"\n')
        out.write(f'printf "CPU cores:     %s\\n"   "{config.num_cpus}"\n')
        out.write(
            f'printf "Memory:        %s GB (%s GB per CPU core)\\n"'
            f' "{config.memory_gb}" "{mem_per_cpu}"\n'
        )
        out.write(f'printf "Time limit:    %s\\n"   "{time_display}"\n')
        out.write('printf "Submitted by:  %s\\n"   "${USER:-}"\n')
        out.write('printf "Submitted on:  %s\\n"   "$(date)"\n')
        out.write("\n")

    def _emit_turbomole_run_command(
        self,
        out: io.StringIO,
        config: RuntimeConfig,
        *,
        array_mode: bool,
    ) -> None:
        """Emit environment export, cd, and dscf invocation.

        Args:
            out: Output buffer.
            config: Runtime config.
            array_mode: Whether generating for array mode.
        """
        out.write(f'export PARNODES="{config.num_cpus}"\n')
        out.write(f'export OMP_NUM_THREADS="{config.num_cpus}"\n')
        out.write("\n")
        out.write('cd "$control_dir" || exit 1\n')
        out.write("\n")
        out.write('dscf > "$output_file" 2>&1 && EXIT_CODE=0 || EXIT_CODE=$?\n')

    def _build_from_tokens(self, tokens: list[str]) -> None:
        """Parse control/coord pairs from positional tokens.

        Args:
            tokens: File tokens.
        """
        assert isinstance(tokens, list), "tokens must be a list"
        assert len(tokens) > 0, "tokens must not be empty"
        current_control = ""

        for tok in tokens:
            if tok.endswith("control"):
                validate_file_exists(tok)
                current_control = tok
            elif tok.endswith("coord"):
                validate_file_exists(tok)
                if not current_control:
                    die_usage(f"Coord file without preceding control: {tok}")
                self._jobs.append(
                    f"{to_absolute_path(current_control)}\t{to_absolute_path(tok)}"
                )
            else:
                die_usage(f"Unsupported file (expect *control or *coord): {tok}")

        if not self._jobs:
            die_usage("No control/coord pairs specified")

    def _read_manifest(self, filepath: str) -> None:
        """Read tab-separated control/coord manifest.

        Args:
            filepath: Manifest file path.
        """
        assert filepath, "filepath must be non-empty"
        validate_file_exists(filepath)
        with open(filepath) as fh:
            for line in fh:
                line = line.rstrip("\r\n")
                if not line.strip() or line.strip().startswith("#"):
                    continue
                parts = line.split("\t")
                control = parts[0] if len(parts) > 0 else ""
                coord = parts[1] if len(parts) > 1 else ""
                if not os.path.isfile(control):
                    die_usage(f"Invalid control in manifest: {control}")
                if not os.path.isfile(coord):
                    die_usage(f"Invalid coord in manifest: {coord}")
                self._jobs.append(f"{control}\t{coord}")

    @staticmethod
    def _parse_job_line(line: str) -> tuple[str, str]:
        """Parse a tab-separated job line into (control, coord).

        Args:
            line: Tab-separated job line.

        Returns:
            Tuple of (control, coord).
        """
        parts = line.split("\t")
        return parts[0], parts[1] if len(parts) > 1 else ""
