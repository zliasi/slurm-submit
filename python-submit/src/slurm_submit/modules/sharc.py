"""Module: SHARC (Surface Hopping including ARbitrary Couplings).

Category A: single-file, scratch, archive, copies INITCONDS/QM dirs.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from slurm_submit.core import die_usage, strip_extension
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="sharc",
    input_extensions=(".inp",),
    output_extensions=(),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=5,
    default_output_dir="output",
    uses_scratch=True,
    uses_archive=True,
    memory_unit="gb",
)


@register_module("sharc")
class SharcModule(SubmitModule):
    """SHARC submission module."""

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " SHARC submission\n"
            "\n"
            " Examples:\n"
            "   ssharc dynamics.inp -c 4 -m 8 -t 2-00:00:00\n"
            "   ssharc traj_*.inp --throttle 10 -c 2 -m 4"
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
        """Emit SHARC run command with input + aux dir copy.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.out is not None, "Output buffer must not be None"
        assert ctx.input_ref, "Input reference must not be empty"

        ctx.out.write(f'input_dir=$(dirname "{ctx.input_ref}")\n')
        ctx.out.write(f'cp "{ctx.input_ref}" "$scratch_directory/input"\n')
        ctx.out.write('if [[ -f "$input_dir/INITCONDS" ]]; then\n')
        ctx.out.write('  cp "$input_dir/INITCONDS" "$scratch_directory/"\n')
        ctx.out.write("fi\n")
        ctx.out.write('if [[ -d "$input_dir/QM" ]]; then\n')
        ctx.out.write('  cp -r "$input_dir/QM" "$scratch_directory/"\n')
        ctx.out.write("fi\n")
        ctx.out.write("\n")
        ctx.out.write('cd "$scratch_directory"\n')
        ctx.out.write("$SHARC/sharc.x input > output.log 2>&1\n")

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """Emit retrieval of output.dat, output.log, restart, etc.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """
        assert out is not None, "Output buffer must not be None"
        assert stem_ref, "Stem reference must not be empty"

        out.write('if [[ -f "output.dat" ]]; then\n')
        out.write(f"  cp output.dat" f' "${{output_directory}}{stem_ref}_output.dat"\n')
        out.write("fi\n")
        out.write('if [[ -f "output.log" ]]; then\n')
        out.write(f"  cp output.log" f' "${{output_directory}}{stem_ref}_output.log"\n')
        out.write("fi\n")
        out.write('if [[ -d "restart" ]]; then\n')
        out.write(f"  cp -r restart" f' "${{output_directory}}{stem_ref}_restart"\n')
        out.write("fi\n")
        out.write("for file in *.out *.xyz *.dat; do\n")
        out.write('  if [[ -f "$file" ]]; then\n')
        out.write('    cp "$file" "${output_directory}"\n')
        out.write("  fi\n")
        out.write("done\n")
        out.write("cd /\n")

    def job_name(self, input_file: str) -> str:
        """Compute job name.

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
        targets = [
            f"{output_dir}{stem}_output.dat",
            f"{output_dir}{stem}_output.log",
            f"{output_dir}{stem}{config.log_extension}",
        ]
        if config.create_archive:
            targets.append(f"{output_dir}{stem}.tar.gz")
        return targets
