"""Module: Gaussian 16.

Category A: single-file, scratch, retrieve .chk.
"""

from __future__ import annotations

import io
import logging
import os
from typing import TYPE_CHECKING

from slurm_submit.core import die_usage
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="gaussian",
    input_extensions=(".com", ".gjf"),
    output_extensions=(".out",),
    retrieve_extensions=(".chk",),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=10,
    default_output_dir="output",
    uses_scratch=True,
    uses_archive=False,
    memory_unit="gb",
)


@register_module("gaussian")
class GaussianModule(SubmitModule):
    """Gaussian 16 submission module."""

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " Gaussian 16 submission\n"
            "\n"
            " Examples:\n"
            "   sgaussian opt_b3lyp_ccpvdz_h2o.com -c 2 -m 4"
            " -t 04:00:00\n"
            "   sgaussian *.com --throttle 5 -c 2 -m 4"
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
        """Emit gaussian execution.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.out is not None, "Output buffer must not be None"
        assert ctx.stem_ref, "Stem reference must not be empty"

        gaussian_exec = ctx.software.paths.get("gaussian_exec", "g16")
        ctx.out.write('export GAUSS_SCRDIR="$scratch_directory"\n')
        ctx.out.write(
            f'srun {gaussian_exec} "{ctx.input_ref}"'
            f' > "${{output_directory}}{ctx.stem_ref}.out"\n'
        )

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """Emit .chk retrieval.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """
        assert out is not None, "Output buffer must not be None"
        assert stem_ref, "Stem reference must not be empty"

        out.write(f"if ls {stem_ref}.chk 1>/dev/null 2>&1; then\n")
        out.write(f"  mv {stem_ref}.chk" f' "${{output_directory}}{stem_ref}.chk"\n')
        out.write("fi\n")

    def job_name(self, input_file: str) -> str:
        """Strip any extension (.com or .gjf).

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
            f"{output_dir}{stem}.chk",
        ]
