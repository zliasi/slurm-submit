"""Module: NWChem.

Category A: single-file, mpirun, module load.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from slurm_submit.core import die_usage, strip_extension
from slurm_submit.manifest import default_backup_targets
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="nwchem",
    input_extensions=(".nw",),
    output_extensions=(".out",),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=5,
    default_output_dir="output",
    uses_scratch=False,
    uses_archive=False,
    memory_unit="gb",
)


@register_module("nwchem")
class NWChemModule(SubmitModule):
    """NWChem submission module."""

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " NWChem submission\n"
            "\n"
            " Examples:\n"
            "   snwchem opt_dft.nw -c 4 -m 8\n"
            "   snwchem *.nw --throttle 5 -c 2 -m 4"
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
        """Emit nwchem execution.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.out is not None, "Output buffer must not be None"
        assert ctx.stem_ref, "Stem reference must not be empty"

        nwchem_exec = ctx.software.paths.get("nwchem_exec", "nwchem")
        ctx.out.write(
            f"mpirun -np $SLURM_CPUS_ON_NODE {nwchem_exec}"
            f' "{ctx.input_ref}" \\\n'
            f'  > "${{output_directory:-}}{ctx.stem_ref}.out" 2>&1\n'
        )

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
        """Compute job name.

        Args:
            input_file: Input file path.

        Returns:
            Job name.
        """
        return strip_extension(input_file, ".nw")

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
        return default_backup_targets(
            stem,
            output_dir,
            _META.output_extensions,
            config.log_extension,
            config.create_archive and _META.uses_archive,
        )
