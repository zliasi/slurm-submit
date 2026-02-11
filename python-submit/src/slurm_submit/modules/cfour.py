"""Module: CFOUR.

Category A: single-file, scratch, runtime backup, tar.gz archive.
Complex scratch setup: copies binaries, GENBAS, ECPDATA.
"""

from __future__ import annotations

import io
import logging
import os
from typing import TYPE_CHECKING

from slurm_submit.core import (
    die_usage,
    require_arg_value,
    to_absolute_path,
    validate_file_exists,
)
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="cfour",
    input_extensions=(".inp",),
    output_extensions=(".out",),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=10,
    default_output_dir="output",
    uses_scratch=True,
    uses_archive=False,
    memory_unit="gb",
)


@register_module("cfour")
class CfourModule(SubmitModule):
    """CFOUR submission module."""

    def __init__(self) -> None:
        """Initialize module state."""
        self._custom_genbas = ""

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " CFOUR submission\n"
            "\n"
            " Module options:\n"
            "   -g, --genbas FILE   Custom GENBAS file\n"
            "\n"
            " Examples:\n"
            "   scfour scf_ccsd.inp -c 1 -m 4\n"
            "   scfour *.inp -c 2 -m 8 -g custom_GENBAS"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Parse --genbas flag.

        Args:
            args: Remaining args.
            config: Runtime config.
        """
        assert isinstance(args, list), "args must be a list"
        assert config is not None, "Config must not be None"

        i = 0
        while i < len(args):
            if args[i] in ("-g", "--genbas"):
                require_arg_value(args[i], i + 1, len(args))
                self._custom_genbas = args[i + 1]
                i += 2
            else:
                die_usage(f"Unknown option: {args[i]}")

    def validate(self, config: RuntimeConfig) -> None:
        """Validate custom genbas exists if specified.

        Args:
            config: Runtime config.
        """
        if self._custom_genbas:
            validate_file_exists(self._custom_genbas)

    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """Emit environment setup.

        Args:
            out: Output buffer.
            software: Software config.
        """
        if software.dependencies:
            out.write(software.dependencies + "\n")

    def emit_run_command(self, ctx: RunContext) -> None:
        """Emit CFOUR run command with full scratch setup.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.out is not None, "Output buffer must not be None"
        assert ctx.stem_ref, "Stem reference must not be empty"

        cfour_dir = ctx.software.paths.get("cfour_dir", "")
        basis_file = ctx.software.paths.get("cfour_basis_file", "")

        genbas_abs = ""
        if self._custom_genbas:
            genbas_abs = to_absolute_path(self._custom_genbas)

        basis_abs = ""
        if basis_file:
            expanded = os.path.expanduser(basis_file)
            if os.path.isfile(expanded):
                basis_abs = to_absolute_path(expanded)

        self._emit_cfour_env_setup(ctx.out, cfour_dir)
        ctx.out.write(f'cp "{ctx.input_ref}" ZMAT\n')
        ctx.out.write("\n")
        ctx.out.write(f'input_dir=$(dirname "{ctx.input_ref}")\n')
        ctx.out.write("\n")
        self._emit_cfour_basis_copy(ctx.out, basis_abs, genbas_abs)
        self._emit_cfour_execution(ctx.out, ctx.stem_ref, ctx.config)

    @staticmethod
    def _emit_cfour_env_setup(out: io.StringIO, cfour_dir: str) -> None:
        """Emit CFOUR environment variables and scratch cd.

        Args:
            out: Output buffer.
            cfour_dir: Path to CFOUR installation directory.
        """
        assert out is not None, "Output buffer must not be None"
        assert isinstance(cfour_dir, str), "cfour_dir must be a string"

        out.write(f'export CFOUR="{cfour_dir}"\n')
        out.write('export PATH=".:$PATH:$scratch_directory"\n')
        out.write("\n")
        out.write('cd "$scratch_directory"\n')
        out.write("\n")
        out.write('lock_file="/tmp/cfour_copy_${SLURM_JOB_ID}.lock"\n')
        out.write('exec 200>"$lock_file"\n')
        out.write(
            "flock -w 30 200"
            ' || printf "Warning: Could not acquire copy lock\\n"'
            " >&2\n"
        )
        out.write("\n")
        out.write('cp "$CFOUR"/bin/* .\n')

    @staticmethod
    def _emit_cfour_basis_copy(
        out: io.StringIO, basis_abs: str, genbas_abs: str
    ) -> None:
        """Emit GENBAS and ECPDATA copy commands.

        Args:
            out: Output buffer.
            basis_abs: Absolute path to basis file, or empty.
            genbas_abs: Absolute path to custom GENBAS, or empty.
        """
        assert out is not None, "Output buffer must not be None"
        assert isinstance(basis_abs, str), "basis_abs must be a string"

        if basis_abs:
            out.write(f'printf "Using basis file: {basis_abs}\\n"\n')
            out.write(f'cp "{basis_abs}" GENBAS\n')
        elif genbas_abs:
            out.write(f'printf "Using custom GENBAS: {genbas_abs}\\n"\n')
            out.write(f'cp "{genbas_abs}" GENBAS\n')
        else:
            out.write('if [[ -f "${input_dir}/GENBAS" ]]; then\n')
            out.write('  printf "Using GENBAS from input directory\\n"\n')
            out.write('  cp "${input_dir}/GENBAS" GENBAS\n')
            out.write("else\n")
            out.write('  printf "Using default GENBAS from CFOUR\\n"\n')
            out.write('  cp "$CFOUR/basis/GENBAS" .\n')
            out.write("fi\n")

        out.write("\n")
        out.write('cp "$CFOUR/basis/ECPDATA" .\n')
        out.write("\n")
        out.write("flock -u 200\n")
        out.write("exec 200>&-\n")
        out.write('rm -f "$lock_file"\n')
        out.write("\n")

    @staticmethod
    def _emit_cfour_execution(
        out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """Emit xcfour execution and tar archive commands.

        Args:
            out: Output buffer.
            stem_ref: Stem reference for output naming.
            config: Runtime config.
        """
        assert out is not None, "Output buffer must not be None"
        assert stem_ref, "Stem reference must not be empty"

        out.write('printf "%s\\n" "$(hostname)" > nodefile\n')
        out.write("\n")
        out.write(
            "./xcfour ./ZMAT ./GENBAS"
            f' > "${{output_directory}}{stem_ref}.out" \\\n'
            "  && cfour_exit_code=0 || cfour_exit_code=$?\n"
        )
        out.write("\n")
        out.write(
            'if [[ "$output_directory" != ""'
            ' && "$output_directory" != "./" ]]; then\n'
        )
        out.write(
            f'  tar -zcf "${{output_directory}}{stem_ref}.tar.gz"'
            " \\\n"
            "    out* anh* b* c* d* i* j* p* q* zm*"
            " 2>/dev/null || true\n"
        )
        out.write("fi\n")
        out.write("\n")
        out.write("cd ..\n")

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """No outputs to retrieve.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """

    def job_name(self, input_file: str) -> str:
        """Strip extension generically.

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
            f"{output_dir}{stem}.tar.gz",
        ]
