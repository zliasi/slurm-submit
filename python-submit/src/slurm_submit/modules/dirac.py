"""Module: DIRAC (relativistic quantum chemistry).

Category C: multi-file (inp/mol pairs), scratch, runtime backup.
"""

from __future__ import annotations

import io
import logging
import os
from typing import TYPE_CHECKING

from slurm_submit.backup import backup_existing_file, emit_backup_function_inline
from slurm_submit.core import (
    die_usage,
    strip_extension,
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
    name="dirac",
    output_extensions=(".out",),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=10,
    default_output_dir="output",
    uses_scratch=True,
    uses_archive=False,
    memory_unit="gb",
)


@register_module("dirac")
class DiracModule(SubmitModule):
    """DIRAC submission module."""

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
            " DIRAC submission (paired inp/mol)\n"
            "\n"
            " Usage:\n"
            "   sdirac input.inp geom.mol [options]\n"
            "   sdirac inp1.inp mol1.mol inp2.inp mol2.mol ... [options]\n"
            "   sdirac -M FILE [options]\n"
            "\n"
            " Examples:\n"
            "   sdirac sp-hf.inp 631g-h2o.mol -c 2 -m 4\n"
            "   sdirac sp-hf.inp h2o.mol sp-mp2.inp h2o.mol -c 4 -m 8\n"
            "   sdirac -M manifest.txt -T 5"
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
        """No-op for dirac.

        Args:
            ctx: Run context.
        """

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """No-op for dirac.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """

    def job_name(self, input_file: str) -> str:
        """Strip .inp extension.

        Args:
            input_file: Input file path.

        Returns:
            Job name.
        """
        return strip_extension(input_file, ".inp")

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
        """Build jobs from paired inp/mol tokens or manifest.

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
            return f"dirac-array-{len(self._jobs)}t{config.throttle}"
        inp, mol = self._parse_job_line(self._jobs[0])
        inp_base = os.path.splitext(os.path.basename(inp))[0]
        mol_base = os.path.splitext(os.path.basename(mol))[0]
        return f"{inp_base}_{mol_base}"

    def backup_all(self, config: RuntimeConfig) -> None:
        """Backup all outputs for jobs.

        Args:
            config: Runtime config.
        """
        assert self._jobs, "jobs must be non-empty"
        for line in self._jobs:
            inp, mol = self._parse_job_line(line)
            inp_base = os.path.splitext(os.path.basename(inp))[0]
            mol_base = os.path.splitext(os.path.basename(mol))[0]
            stem = f"{inp_base}_{mol_base}"
            backup_existing_file(
                f"{config.output_dir}{stem}.out",
                config.use_backup_dir,
                config.backup_dir_name,
                config.max_backups,
            )
            if config.array_mode:
                backup_existing_file(
                    f"{config.output_dir}{stem}{config.log_extension}",
                    config.use_backup_dir,
                    config.backup_dir_name,
                    config.max_backups,
                )

    def generate_array_body(self, out: io.StringIO, ctx: ScriptContext) -> None:
        """Generate array job body for dirac.

        Args:
            out: Output buffer.
            ctx: Script context.
        """
        assert ctx.exec_manifest, "exec_manifest must be set for array mode"
        assert ctx.config.partition, "partition must be set"
        config = ctx.config
        exec_manifest = to_absolute_path(ctx.exec_manifest)
        mem_per_cpu = config.memory_gb
        total_mem = str(int(config.memory_gb) * config.num_cpus)

        out.write("\n")
        emit_backup_function_inline(
            out, config.use_backup_dir, config.backup_dir_name, config.max_backups
        )

        out.write(f'\nline=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" "{exec_manifest}")\n')
        out.write("IFS=$'\\t' read -r INP MOL <<< \"$line\"\n")
        out.write("\n")
        out.write('inp_base=$(basename "$INP"); inp_base="${inp_base%.*}"\n')
        out.write('mol_base=$(basename "$MOL"); mol_base="${mol_base%.*}"\n')
        out.write('stem="${inp_base}_${mol_base}"\n')
        out.write("\n")
        out.write(f'output_file="{config.output_dir}${{stem}}.out"\n')
        out.write(f'log_file="{config.output_dir}${{stem}}{config.log_extension}"\n')
        out.write("\n")
        out.write('exec 1>"$log_file" 2>&1\n')

        self._emit_dirac_info_block(
            out, ctx, total_mem, str(mem_per_cpu), array_mode=True
        )

        out.write('backup_existing_files "$output_file"\n')
        out.write("\n")
        self._emit_dirac_run_command(
            out,
            config,
            ctx.software,
            total_mem,
            str(mem_per_cpu),
            array_mode=True,
        )

        emit_job_footer(out, True)
        out.write("\nexit $dirac_exit_code\n")

    def generate_single_body(self, out: io.StringIO, ctx: ScriptContext) -> None:
        """Generate single job body for dirac.

        Args:
            out: Output buffer.
            ctx: Script context.
        """
        assert self._jobs, "jobs must be non-empty"
        assert ctx.config.partition, "partition must be set"
        config = ctx.config
        software = ctx.software
        inp, mol = self._parse_job_line(self._jobs[0])
        inp_base = os.path.splitext(os.path.basename(inp))[0]
        mol_base = os.path.splitext(os.path.basename(mol))[0]
        stem = f"{inp_base}_{mol_base}"
        mem_per_cpu = config.memory_gb
        total_mem = str(int(config.memory_gb) * config.num_cpus)

        out.write("\n")
        emit_backup_function_inline(
            out, config.use_backup_dir, config.backup_dir_name, config.max_backups
        )

        out.write(f'\nstem="{stem}"\n')
        out.write(f'output_file="{config.output_dir}${{stem}}.out"\n')

        self._emit_dirac_info_block(
            out, ctx, total_mem, str(mem_per_cpu), array_mode=False
        )

        out.write('backup_existing_files "$output_file"\n')
        out.write("\n")
        self._emit_dirac_run_command(
            out,
            config,
            software,
            total_mem,
            str(mem_per_cpu),
            array_mode=False,
            inp=inp,
            mol=mol,
            inp_base=inp_base,
            mol_base=mol_base,
        )

        emit_job_footer(out, False)
        out.write("\nexit $dirac_exit_code\n")

    def _emit_dirac_info_block(
        self,
        out: io.StringIO,
        ctx: ScriptContext,
        total_mem: str,
        mem_per_cpu: str,
        *,
        array_mode: bool,
    ) -> None:
        """Emit the job information printf block.

        Args:
            out: Output buffer.
            ctx: Script context.
            total_mem: Total memory string.
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
            f' "{total_mem}" "{mem_per_cpu}"\n'
        )
        out.write(f'printf "Time limit:    %s\\n"   "{time_display}"\n')
        out.write('printf "Submitted by:  %s\\n"   "${USER:-}"\n')
        out.write('printf "Submitted on:  %s\\n"   "$(date)"\n')
        out.write("\n")

    def _emit_dirac_run_command(
        self,
        out: io.StringIO,
        config: RuntimeConfig,
        software: SoftwareConfig,
        total_mem: str,
        mem_per_cpu: str,
        *,
        array_mode: bool,
        inp: str = "",
        mol: str = "",
        inp_base: str = "",
        mol_base: str = "",
    ) -> None:
        """Emit scratch setup, pam invocation, output move, and cleanup.

        Args:
            out: Output buffer.
            config: Runtime config.
            software: Software config.
            total_mem: Total memory string.
            mem_per_cpu: Memory per CPU string.
            array_mode: Whether generating for array mode.
            inp: Input file path (single mode).
            mol: Mol file path (single mode).
            inp_base: Input basename without extension (single mode).
            mol_base: Mol basename without extension (single mode).
        """
        dirac_pam = software.paths.get("dirac_pam", "pam")

        if array_mode:
            out.write(
                f'export DIRAC_SCRATCH="{config.scratch_base}'
                f'/${{SLURM_ARRAY_JOB_ID}}/${{SLURM_ARRAY_TASK_ID}}"\n'
            )
        else:
            out.write(
                f'export DIRAC_SCRATCH="{config.scratch_base}/${{SLURM_JOB_ID}}"\n'
            )
        out.write('mkdir -p "$DIRAC_SCRATCH"\n')
        out.write("\n")
        out.write(f'"{dirac_pam}" \\\n')
        out.write(f'  --mpi="{config.num_cpus}" \\\n')
        out.write(f'  --ag="{total_mem}" \\\n')
        out.write(f'  --gb="{mem_per_cpu}" \\\n')
        out.write('  --scratch="$DIRAC_SCRATCH" \\\n')
        if array_mode:
            out.write('  --mol="$MOL" \\\n')
            out.write('  --inp="$INP" \\\n')
        else:
            out.write(f'  --mol="{mol}" \\\n')
            out.write(f'  --inp="{inp}" \\\n')
        out.write("  && dirac_exit_code=0 || dirac_exit_code=$?\n")
        out.write("\n")
        out.write(
            f'if [[ "{config.output_dir}" != ""'
            f' && "{config.output_dir}" != "./" ]]; then\n'
        )
        if array_mode:
            out.write('  for file in "${inp_base}_${mol_base}"*; do\n')
        else:
            out.write(f'  for file in "{inp_base}_{mol_base}"*; do\n')
        out.write(
            '    if [[ -f "$file" && "$file" != *.inp && "$file" != *.mol ]]; then\n'
        )
        out.write(f'      mv "$file" "{config.output_dir}" 2>/dev/null || true\n')
        out.write("    fi\n")
        out.write("  done\n")
        out.write("fi\n")
        out.write("\n")
        out.write('rm -rf "$DIRAC_SCRATCH" || true\n')

    def _build_from_tokens(self, tokens: list[str]) -> None:
        """Parse paired inp/mol tokens.

        Args:
            tokens: File tokens (alternating inp mol).
        """
        assert isinstance(tokens, list), "tokens must be a list"
        if len(tokens) < 2:
            die_usage("DIRAC requires both an .inp and .mol file")
        if len(tokens) % 2 != 0:
            die_usage("Each .inp file must be paired with a .mol file")
        assert len(tokens) >= 2, "need at least one inp/mol pair after validation"

        for i in range(0, len(tokens), 2):
            inp, mol = tokens[i], tokens[i + 1]
            validate_file_exists(inp)
            validate_file_exists(mol)
            if not inp.endswith(".inp"):
                die_usage(f"Expected .inp file, got: {inp}")
            if not mol.endswith(".mol"):
                die_usage(f"Expected .mol file, got: {mol}")
            self._jobs.append(f"{to_absolute_path(inp)}\t{to_absolute_path(mol)}")

    def _read_manifest(self, filepath: str) -> None:
        """Read tab-separated inp/mol manifest.

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
                inp = parts[0] if len(parts) > 0 else ""
                mol = parts[1] if len(parts) > 1 else ""
                if not (inp.endswith(".inp") and os.path.isfile(inp)):
                    die_usage(f"Invalid INP in manifest: {inp}")
                if not (mol.endswith(".mol") and os.path.isfile(mol)):
                    die_usage(f"Invalid MOL in manifest: {mol}")
                self._jobs.append(f"{inp}\t{mol}")

    @staticmethod
    def _parse_job_line(line: str) -> tuple[str, str]:
        """Parse a tab-separated job line into (inp, mol).

        Args:
            line: Tab-separated job line.

        Returns:
            Tuple of (inp, mol).
        """
        parts = line.split("\t")
        return parts[0], parts[1] if len(parts) > 1 else ""
