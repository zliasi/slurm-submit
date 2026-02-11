"""Module: Orca quantum chemistry.

Category A: simple single-file, scratch + archive + retrieve.
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
    name="orca",
    input_extensions=(".inp",),
    output_extensions=(".out",),
    retrieve_extensions=(".xyz", ".nto", ".cube"),
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=5,
    default_output_dir="output",
    uses_scratch=True,
    uses_archive=True,
    memory_unit="gb",
)


@register_module("orca")
class OrcaModule(SubmitModule):
    """Orca quantum chemistry submission module."""

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " Orca submission\n"
            "\n"
            " Examples:\n"
            "   sorca opt_b3lyp_def2tzvp.inp -c 8 -m 16 -p kemi6\n"
            "   sorca *.inp --throttle 5 -c 4 -m 8\n"
            "   sorca -M manifest.txt --throttle 2 -c 4 -m 8 -p chem"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Reject any unknown args.

        Args:
            args: Remaining args after common parsing.
            config: Runtime config.
        """
        if args:
            die_usage(f"Unknown option: {args[0]}")

    def validate(self, config: RuntimeConfig) -> None:
        """No extra validation needed.

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
        """Emit orca execution command.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.out is not None, "Output buffer must not be None"
        assert ctx.stem_ref, "Stem reference must not be empty"

        orca_path = ctx.software.paths.get("orca_path", "orca")
        ctx.out.write(
            f'cp "{ctx.input_ref}"' f' "$scratch_directory/{ctx.stem_ref}.inp"\n'
        )
        ctx.out.write(
            f"{orca_path}/orca"
            f' "$scratch_directory/{ctx.stem_ref}.inp"'
            f' > "${{output_directory}}{ctx.stem_ref}.out"\n'
        )

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """Emit file retrieval from scratch.

        Args:
            out: Output buffer.
            stem_ref: Stem reference.
            config: Runtime config.
        """
        assert out is not None, "Output buffer must not be None"
        assert stem_ref, "Stem reference must not be empty"

        out.write('printf "\\n"\n')
        out.write("for ext in .xyz .nto .cube; do\n")
        out.write("  while IFS= read -r -d '' file; do\n")
        out.write('    filename=$(basename "$file")\n')
        out.write('    if mv "$file" "${output_directory}$filename"; then\n')
        out.write('      printf "Retrieved: %s\\n" "$filename"\n')
        out.write("    else\n")
        out.write('      printf "Warning: Failed to retrieve %s\\n"' ' "$filename"\n')
        out.write("    fi\n")
        out.write(
            '  done < <(find "$scratch_directory" -maxdepth 1'
            " -type f \\\n"
            '    -name "*$ext" -print0)\n'
        )
        out.write("done\n")

    def job_name(self, input_file: str) -> str:
        """Compute job name from input file.

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
            File paths to backup.
        """
        return default_backup_targets(
            stem,
            output_dir,
            _META.output_extensions,
            config.log_extension,
            config.create_archive and _META.uses_archive,
        )
