"""Module: Dalton.

Category C: multi-file (dal/mol/pot/rst), scratch, runtime backup,
sticky pot, 32i/64i binary, LoProp.
"""

from __future__ import annotations

import io
import logging
import os
import re
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
    name="dalton",
    output_extensions=(".out",),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=10,
    default_output_dir="output",
    uses_scratch=True,
    uses_archive=False,
    memory_unit="gb",
)


def _dal_contains_geometry(dal_file: str) -> bool:
    """Check if DAL file contains embedded geometry.

    Args:
        dal_file: Path to DAL file.

    Returns:
        True if file contains geometry.
    """
    assert dal_file, "dal_file must be non-empty"
    assert isinstance(dal_file, str), "dal_file must be a string"
    try:
        with open(dal_file) as fh:
            for line in fh:
                if re.match(r"^BASIS|^Atomtypes=", line):
                    return True
    except OSError:
        pass
    return False


def _dalton_stem(dal: str, mol: str, pot: str) -> str:
    """Compute dalton stem from job fields.

    Args:
        dal: DAL path.
        mol: MOL path (may be empty).
        pot: POT path (may be empty).

    Returns:
        Stem string.
    """
    assert dal, "dal path must be non-empty"
    assert isinstance(dal, str), "dal must be a string"
    dal_base = os.path.splitext(os.path.basename(dal))[0]
    if mol:
        mol_base = os.path.splitext(os.path.basename(mol))[0]
        if pot:
            pot_base = os.path.splitext(os.path.basename(pot))[0]
            return f"{dal_base}_{mol_base}_{pot_base}"
        return f"{dal_base}_{mol_base}"
    return dal_base


@register_module("dalton")
class DaltonModule(SubmitModule):
    """Dalton submission module."""

    def __init__(self) -> None:
        """Initialize module state."""
        self._loprop = False
        self._jobs: list[str] = []

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " Dalton submission (dal/mol/pot/rst)\n"
            "\n"
            " Module options:\n"
            '   -l, --loprop             Request LoProp files (-get "AOONEINT AOPROPER")\n'
            "\n"
            " Usage:\n"
            "   sdalton input.dal geom.mol [pot.pot] [restart.tar.gz] [options]\n"
            "   sdalton input.dal geom1.mol geom2.mol ... [options]\n"
            "   sdalton -M FILE [options]\n"
            "\n"
            " Examples:\n"
            "   sdalton exc_b3lyp.dal augccpvdz_h2o.mol -c 1 -m 4\n"
            "   sdalton opt.dal ccpvdz_h2o.mol ccpvdz_ethanol.mol -T 5\n"
            "   sdalton -M manifest.txt -c 4 -m 8"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Parse --loprop flag.

        Args:
            args: Remaining args.
            config: Runtime config.
        """
        i = 0
        while i < len(args):
            if args[i] in ("-l", "--loprop"):
                self._loprop = True
                i += 1
            else:
                die_usage(f"Unknown option: {args[i]}")

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
        """No-op for dalton (uses custom body generators).

        Args:
            ctx: Run context.
        """

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """No-op for dalton.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """

    def job_name(self, input_file: str) -> str:
        """Strip .dal extension.

        Args:
            input_file: Input file path.

        Returns:
            Job name.
        """
        from slurm_submit.core import strip_extension

        return strip_extension(input_file, ".dal")

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
        """Build jobs from positional tokens or manifest.

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
            return f"dalton-array-{len(self._jobs)}t{config.throttle}"
        dal, mol, pot, _ = self._parse_job_line(self._jobs[0])
        return _dalton_stem(dal, mol, pot)

    def backup_all(self, config: RuntimeConfig) -> None:
        """Backup all outputs for jobs.

        Args:
            config: Runtime config.
        """
        assert self._jobs, "jobs must be non-empty"
        for line in self._jobs:
            dal, mol, pot, _ = self._parse_job_line(line)
            stem = _dalton_stem(dal, mol, pot)
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
        """Generate array job body for dalton.

        Args:
            out: Output buffer.
            ctx: Script context.
        """
        assert ctx.exec_manifest, "exec_manifest must be set for array mode"
        assert ctx.config.partition, "partition must be set"
        config = ctx.config
        software = ctx.software
        exec_manifest = to_absolute_path(ctx.exec_manifest)
        mem_per_cpu = str(int(config.memory_gb) // config.num_cpus)

        out.write("\n")
        emit_backup_function_inline(
            out, config.use_backup_dir, config.backup_dir_name, config.max_backups
        )

        out.write(f'\nline=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" "{exec_manifest}")\n')
        out.write("IFS=$'\\t' read -r DAL MOL POT RST <<< \"$line\"\n")
        self._emit_dalton_stem_computation(out, array_mode=True)

        out.write(f'output_file="{config.output_dir}${{stem}}.out"\n')
        out.write(f'log_file="{config.output_dir}${{stem}}{config.log_extension}"\n')
        out.write("\n")
        out.write('exec 1>"$log_file" 2>&1\n')

        self._emit_dalton_info_block(out, ctx, mem_per_cpu, array_mode=True)

        out.write('backup_existing_files "$output_file"\n')
        out.write("\n")
        self._emit_dalton_run_command(
            out, config, software, mem_per_cpu, array_mode=True
        )

        emit_job_footer(out, True)
        out.write("\nexit $dalton_exit_code\n")

    def generate_single_body(self, out: io.StringIO, ctx: ScriptContext) -> None:
        """Generate single job body for dalton.

        Args:
            out: Output buffer.
            ctx: Script context.
        """
        assert self._jobs, "jobs must be non-empty"
        assert ctx.config.partition, "partition must be set"
        config = ctx.config
        software = ctx.software
        dal, mol, pot, rst = self._parse_job_line(self._jobs[0])
        _dalton_stem(dal, mol, pot)
        mem_per_cpu = str(int(config.memory_gb) // config.num_cpus)

        out.write("\n")
        emit_backup_function_inline(
            out, config.use_backup_dir, config.backup_dir_name, config.max_backups
        )

        out.write(f'\nDAL="{dal}"\n')
        out.write(f'MOL="{mol}"\n')
        out.write(f'POT="{pot}"\n')
        out.write(f'RST="{rst}"\n')
        self._emit_dalton_stem_computation(out, array_mode=False)

        out.write(f'output_file="{config.output_dir}${{stem}}.out"\n')

        self._emit_dalton_info_block(out, ctx, mem_per_cpu, array_mode=False)

        out.write('backup_existing_files "$output_file"\n')
        out.write("\n")
        self._emit_dalton_run_command(
            out, config, software, mem_per_cpu, array_mode=False
        )

        emit_job_footer(out, False)
        out.write("\nexit $dalton_exit_code\n")

    def _emit_dalton_stem_computation(
        self, out: io.StringIO, *, array_mode: bool
    ) -> None:
        """Emit shell lines that compute the stem variable from DAL/MOL/POT.

        Args:
            out: Output buffer.
            array_mode: Whether generating for array mode.
        """
        out.write("\n")
        out.write('dal_base=$(basename "$DAL"); dal_base="${dal_base%.*}"\n')
        if array_mode:
            out.write('mol_base=$(basename "$MOL"); mol_base="${mol_base%.*}"\n')
            out.write('if [[ -n "$POT" ]]; then\n')
            out.write('  pot_base=$(basename "$POT"); pot_base="${pot_base%.*}"\n')
            out.write('  stem="${dal_base}_${mol_base}_${pot_base}"\n')
            out.write("else\n")
            out.write('  stem="${dal_base}_${mol_base}"\n')
            out.write("fi\n")
        else:
            out.write('if [[ -n "$MOL" ]]; then\n')
            out.write('  mol_base=$(basename "$MOL"); mol_base="${mol_base%.*}"\n')
            out.write('  if [[ -n "$POT" ]]; then\n')
            out.write('    pot_base=$(basename "$POT"); pot_base="${pot_base%.*}"\n')
            out.write('    stem="${dal_base}_${mol_base}_${pot_base}"\n')
            out.write("  else\n")
            out.write('    stem="${dal_base}_${mol_base}"\n')
            out.write("  fi\n")
            out.write("else\n")
            out.write('  stem="${dal_base}"\n')
            out.write("fi\n")
        out.write("\n")

    def _emit_dalton_info_block(
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

    def _emit_dalton_run_command(
        self,
        out: io.StringIO,
        config: RuntimeConfig,
        software: SoftwareConfig,
        mem_per_cpu: str,
        *,
        array_mode: bool,
    ) -> None:
        """Emit scratch setup, dalton invocation, output move, and cleanup.

        Args:
            out: Output buffer.
            config: Runtime config.
            software: Software config.
            mem_per_cpu: Memory per CPU string.
            array_mode: Whether generating for array mode.
        """
        dalton_32i = software.paths.get("dalton_exec_32i", "dalton")
        dalton_64i = software.paths.get("dalton_exec_64i", "dalton")

        if array_mode:
            out.write(
                f'export DALTON_TMPDIR="{config.scratch_base}'
                f'/${{SLURM_ARRAY_JOB_ID}}/${{SLURM_ARRAY_TASK_ID}}"\n'
            )
        else:
            out.write(
                f'export DALTON_TMPDIR="{config.scratch_base}/${{SLURM_JOB_ID}}"\n'
            )
        out.write('mkdir -p "$DALTON_TMPDIR"\n')
        out.write("\n")
        out.write(f'DALTON_BIN="{dalton_32i}"\n')
        out.write(f'if [[ -z "$POT" && "{config.memory_gb}" -gt 16 ]]; then\n')
        out.write(f'  DALTON_BIN="{dalton_64i}"\n')
        out.write("fi\n")
        out.write("\n")
        out.write(
            f'cmd=( "$DALTON_BIN" -d -np "{config.num_cpus}"'
            f' -gb "{config.memory_gb}"'
            f' -t "$DALTON_TMPDIR" -dal "$DAL" -o "$output_file" )\n'
        )
        out.write('[[ -n "$MOL" ]] && cmd+=( -mol "$MOL" )\n')
        out.write('[[ -n "$POT" ]] && cmd+=( -pot "$POT" )\n')
        out.write('[[ -n "$RST" ]] && cmd+=( -f "$RST" )\n')

        if self._loprop:
            out.write('cmd+=( -get "AOONEINT AOPROPER" )\n')

        out.write("\n")
        out.write('"${cmd[@]}" && dalton_exit_code=0 || dalton_exit_code=$?\n')
        out.write("\n")
        out.write(
            f'if [[ "{config.output_dir}" != ""'
            f' && "{config.output_dir}" != "./" ]]; then\n'
        )
        out.write('  if ls "${stem}.tar.gz" 1>/dev/null 2>&1; then\n')
        out.write(f'    mv "${{stem}}.tar.gz" "{config.output_dir}${{stem}}.tar.gz"\n')
        out.write("  fi\n")
        out.write("fi\n")
        out.write("\n")
        out.write('rm -rf "$DALTON_TMPDIR" || true\n')

    def _append_job(self, dal: str, mol: str, pot: str, rst: str) -> None:
        """Append a job entry to internal jobs list.

        Args:
            dal: DAL file path.
            mol: MOL file path (may be empty).
            pot: POT file path (may be empty).
            rst: Restart archive path (may be empty).
        """
        self._jobs.append(f"{dal}\t{mol}\t{pot}\t{rst}")

    def _retroactive_pot(self, pot: str, start_idx: int) -> None:
        """Retroactively apply pot file to jobs from segment start.

        Args:
            pot: POT file path.
            start_idx: Starting index in jobs list.
        """
        assert pot, "pot must be non-empty for retroactive application"
        assert start_idx >= 0, "start_idx must be non-negative"
        for idx in range(start_idx, len(self._jobs)):
            parts = self._jobs[idx].split("\t")
            if len(parts) >= 3 and not parts[2]:
                parts[2] = pot
                self._jobs[idx] = "\t".join(parts)

    def _build_from_tokens(self, tokens: list[str]) -> None:
        """Parse dal/mol/pot/rst tokens with sticky pot logic.

        Args:
            tokens: File tokens.
        """
        assert isinstance(tokens, list), "tokens must be a list"
        assert len(tokens) > 0, "tokens must not be empty"
        current_dal = ""
        current_dal_has_mol = False
        sticky_pot = ""
        seg_start = 0
        next_restart = ""
        pending_global_restart = ""
        pending_pot_before_dal = ""

        for tok in tokens:
            if tok.endswith(".dal"):
                current_dal, current_dal_has_mol, sticky_pot, seg_start = (
                    self._handle_dal_token(tok, pending_pot_before_dal)
                )
                pending_pot_before_dal = ""
                next_restart = ""
            elif tok.endswith(".pot"):
                pending_pot_before_dal, sticky_pot = self._handle_pot_token(
                    tok, current_dal, pending_pot_before_dal, seg_start
                )
            elif tok.endswith(".tar.gz"):
                validate_file_exists(tok)
                abs_rst = to_absolute_path(tok)
                if current_dal:
                    next_restart = abs_rst
                else:
                    pending_global_restart = abs_rst
            elif tok.endswith(".mol"):
                validate_file_exists(tok)
                if not current_dal:
                    die_usage(f".mol without a preceding .dal: {tok}")
                abs_mol = to_absolute_path(tok)
                rst_use = ""
                if next_restart:
                    rst_use = next_restart
                    next_restart = ""
                elif pending_global_restart:
                    rst_use = pending_global_restart
                    pending_global_restart = ""
                self._append_job(current_dal, abs_mol, sticky_pot, rst_use)
                current_dal_has_mol = True
            else:
                die_usage(
                    f"Unsupported file type (expect .dal/.mol/.pot/.tar.gz): {tok}"
                )

        if current_dal and not current_dal_has_mol:
            self._finalize_dal_without_mol(
                current_dal, sticky_pot, next_restart, pending_global_restart
            )

    def _handle_dal_token(
        self, tok: str, pending_pot_before_dal: str
    ) -> tuple[str, bool, str, int]:
        """Process a .dal token during token parsing.

        Args:
            tok: The .dal file token.
            pending_pot_before_dal: Pot file seen before any dal.

        Returns:
            Tuple of (current_dal, current_dal_has_mol, sticky_pot, seg_start).
        """
        validate_file_exists(tok)
        current_dal = to_absolute_path(tok)
        sticky_pot = ""
        if pending_pot_before_dal:
            sticky_pot = pending_pot_before_dal
        return current_dal, False, sticky_pot, len(self._jobs)

    def _handle_pot_token(
        self,
        tok: str,
        current_dal: str,
        pending_pot_before_dal: str,
        seg_start: int,
    ) -> tuple[str, str]:
        """Process a .pot token during token parsing.

        Args:
            tok: The .pot file token.
            current_dal: Currently active dal file.
            pending_pot_before_dal: Pot file seen before any dal.
            seg_start: Start index for current dal segment.

        Returns:
            Tuple of (pending_pot_before_dal, sticky_pot).
        """
        validate_file_exists(tok)
        abs_pot = to_absolute_path(tok)
        if not current_dal:
            return abs_pot, ""
        self._retroactive_pot(abs_pot, seg_start)
        return pending_pot_before_dal, abs_pot

    def _finalize_dal_without_mol(
        self,
        current_dal: str,
        sticky_pot: str,
        next_restart: str,
        pending_global_restart: str,
    ) -> None:
        """Handle a trailing dal with no mol (embedded geometry check).

        Args:
            current_dal: Current dal file path.
            sticky_pot: Current sticky pot path.
            next_restart: Pending restart for this dal.
            pending_global_restart: Global pending restart.
        """
        if _dal_contains_geometry(current_dal):
            rst_use = ""
            if next_restart:
                rst_use = next_restart
            elif pending_global_restart:
                rst_use = pending_global_restart
            self._append_job(current_dal, "", sticky_pot, rst_use)
        else:
            die_usage(
                f".dal file without .mol: {current_dal}"
                " (embed geometry or provide .mol)"
            )

    def _read_manifest(self, filepath: str) -> None:
        """Read tab-separated dal/mol/pot/rst manifest.

        Args:
            filepath: Manifest file path.
        """
        assert filepath, "filepath must be non-empty"
        validate_file_exists(filepath)
        manifest_tokens: list[str] = []

        with open(filepath) as fh:
            for line in fh:
                line = line.rstrip("\r\n")
                if not line.strip() or line.strip().startswith("#"):
                    continue

                fields = line.split()
                if len(fields) >= 2:
                    dal, mol = fields[0], fields[1]
                    pot = fields[2] if len(fields) > 2 else ""
                    rst = fields[3] if len(fields) > 3 else ""

                    if not (dal.endswith(".dal") and os.path.isfile(dal)):
                        die_usage(f"Invalid DAL in manifest: {dal}")
                    if not (mol.endswith(".mol") and os.path.isfile(mol)):
                        die_usage(f"Invalid MOL in manifest: {mol}")
                    if pot and not (pot.endswith(".pot") and os.path.isfile(pot)):
                        die_usage(f"Invalid POT in manifest: {pot}")
                    if rst and not (rst.endswith(".tar.gz") and os.path.isfile(rst)):
                        die_usage(f"Invalid RESTART in manifest: {rst}")

                    self._append_job(dal, mol, pot, rst)
                else:
                    manifest_tokens.append(line)

        if manifest_tokens:
            self._build_from_tokens(manifest_tokens)

    @staticmethod
    def _parse_job_line(line: str) -> tuple[str, str, str, str]:
        """Parse a tab-separated job line into (dal, mol, pot, rst).

        Args:
            line: Tab-separated job line.

        Returns:
            Tuple of (dal, mol, pot, rst).
        """
        parts = line.split("\t")
        dal = parts[0] if len(parts) > 0 else ""
        mol = parts[1] if len(parts) > 1 else ""
        pot = parts[2] if len(parts) > 2 else ""
        rst = parts[3] if len(parts) > 3 else ""
        return dal, mol, pot, rst
