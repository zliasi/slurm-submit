"""Module: xTB (semiempirical quantum chemistry).

Category B: passthrough args, no scratch, float memory.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from slurm_submit.core import (
    require_arg_value,
    strip_extension,
    validate_positive_integer,
)
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="xtb",
    input_extensions=(".xyz",),
    default_cpus=1,
    default_memory_gb=0.5,
    default_throttle=10,
    default_output_dir="output",
    uses_scratch=False,
    uses_archive=False,
    memory_unit="gb_float",
)

_XTB_VALUE_FLAGS = frozenset(
    {
        "--chrg",
        "--uhf",
        "--gfn",
        "--alpb",
        "--gbsa",
        "--namespace",
        "--input",
        "--copy",
        "--restart",
    }
)


@register_module("xtb")
class XtbModule(SubmitModule):
    """xTB submission module."""

    def __init__(self) -> None:
        """Initialize module state."""
        self._xtb_options: list[str] = []
        self._omp_threads = ""

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " xTB submission\n"
            "\n"
            " Module options:\n"
            "   --omp-threads INT   OMP_NUM_THREADS (default: same as --cpus)\n"
            "\n"
            " xTB options (pass through):\n"
            "   --opt, --md, --chrg INT, --uhf INT, --gfn N,"
            " plus any other xtb flags\n"
            "\n"
            " Examples:\n"
            "   sxtb opt.xyz --opt -c 1 -m 0.5\n"
            "   sxtb *.xyz --opt --throttle 5\n"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Parse module-specific + passthrough args.

        Args:
            args: Remaining args.
            config: Runtime config.
        """
        assert isinstance(args, list), "args must be a list"
        assert config is not None, "config must not be None"

        self._xtb_options = []
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "--omp-threads":
                require_arg_value(arg, i + 1, len(args))
                self._omp_threads = args[i + 1]
                i += 2
            elif arg in _XTB_VALUE_FLAGS:
                require_arg_value(arg, i + 1, len(args))
                self._xtb_options.extend([arg, args[i + 1]])
                i += 2
            else:
                self._xtb_options.append(arg)
                i += 1

    def validate(self, config: RuntimeConfig) -> None:
        """Validate omp-threads if set.

        Args:
            config: Runtime config.
        """
        if self._omp_threads:
            validate_positive_integer(self._omp_threads, "omp-threads")

    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """Emit environment setup.

        Args:
            out: Output buffer.
            software: Software config.
        """
        if software.dependencies:
            out.write(software.dependencies + "\n")

    def emit_run_command(self, ctx: RunContext) -> None:
        """Emit xtb run command.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.config.num_cpus >= 1, "num_cpus must be >= 1"
        assert ctx.config.output_dir, "output_dir must not be empty"

        xtb_exec = ctx.software.paths.get("xtb_exec", "xtb")
        omp = self._omp_threads or str(ctx.config.num_cpus)
        opts_str = " ".join(self._xtb_options) if self._xtb_options else ""

        ctx.out.write(f"export OMP_NUM_THREADS={omp}\n")
        ctx.out.write(f'cd "{ctx.config.output_dir}"\n')
        ctx.out.write(
            f'{xtb_exec} "{ctx.input_ref}" {opts_str}'
            f' > "{ctx.stem_ref}{ctx.config.log_extension}" 2>&1\n'
        )

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """Emit output file listing.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """
        assert stem_ref, "stem_ref must not be empty"
        assert config is not None, "config must not be None"

        out.write('printf "\\n"\n')
        out.write('printf "Retrieving output files:\\n"\n')
        out.write(f'for file in "{stem_ref}"*.xyz; do\n')
        out.write('  if [[ -f "$file" ]]; then\n')
        out.write('    printf "Retrieved: %s\\n" "$file"\n')
        out.write("  fi\n")
        out.write("done\n")

    def job_name(self, input_file: str) -> str:
        """Compute job name.

        Args:
            input_file: Input file path.

        Returns:
            Job name.
        """
        return strip_extension(input_file, ".xyz")

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
            f"{output_dir}{stem}{config.log_extension}",
            f"{output_dir}{stem}.opt.xyz",
            f"{output_dir}{stem}.md.xyz",
        ]
