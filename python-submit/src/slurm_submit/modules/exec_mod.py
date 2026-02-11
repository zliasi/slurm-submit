"""Module: exec (generic executable submission).

Category D: no input validation, arbitrary commands.
"""

from __future__ import annotations

import io
import logging
import os
from typing import TYPE_CHECKING

from slurm_submit.core import die_usage, require_arg_value
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="exec",
    default_cpus=1,
    default_memory_gb=2,
    default_throttle=5,
    default_output_dir="output",
    uses_scratch=False,
    uses_archive=False,
    memory_unit="gb",
)


@register_module("exec")
class ExecModule(SubmitModule):
    """Generic executable submission module."""

    def __init__(self) -> None:
        """Initialize module state."""
        self._use_mpi = False
        self._command: list[str] = []

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " Generic executable submission\n"
            "\n"
            " Module options:\n"
            "   --mpi                Use mpirun for parallel execution\n"
            "   -x, --executable PATH  Executable path (alternative to --)\n"
            "\n"
            " Usage:\n"
            "   sexec [options] -- command [args]\n"
            "   sexec [options] -x executable [args]\n"
            "\n"
            " Examples:\n"
            "   sexec -c 4 -m 8 -- ./myprogram arg1 arg2\n"
            "   sexec -c 2 -m 4 --mpi -x ./parallel_program input.dat\n"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Parse exec-specific args (-- , -x, --mpi).

        Args:
            args: Remaining args.
            config: Runtime config.
        """
        assert isinstance(args, list), "args must be a list"
        assert config is not None, "config must not be None"

        executable_path = ""
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "--mpi":
                self._use_mpi = True
                i += 1
            elif arg in ("-x", "--executable"):
                require_arg_value(arg, i + 1, len(args))
                executable_path = args[i + 1]
                i += 2
            elif arg == "--":
                self._command.extend(args[i + 1 :])
                break
            else:
                self._command.append(arg)
                i += 1

        if executable_path:
            self._command = [executable_path] + self._command

    def validate(self, config: RuntimeConfig) -> None:
        """Validate that a command was specified.

        Args:
            config: Runtime config.
        """
        if not self._command:
            die_usage("No command specified (use -- command or -x executable)")

    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """Emit environment setup.

        Args:
            out: Output buffer.
            software: Software config.
        """
        if software.dependencies:
            out.write(software.dependencies + "\n")

    def emit_run_command(self, ctx: RunContext) -> None:
        """Emit the user command execution.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert self._command, "command must not be empty"
        assert ctx.config.output_dir, "output_dir must not be empty"

        cmd_parts = [self._command[0]]
        for arg in self._command[1:]:
            cmd_parts.append(f"'{arg}'")
        cmd_line = " ".join(cmd_parts)

        ctx.out.write(f'cd "{ctx.config.output_dir}"\n')
        if self._use_mpi:
            ctx.out.write(f"mpirun -np $SLURM_NTASKS {cmd_line}\n")
        else:
            ctx.out.write(f"{cmd_line}\n")
        ctx.out.write("EXIT_CODE=$?\n")

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
        """Use command basename as job name.

        Args:
            input_file: Unused, uses command instead.

        Returns:
            Job name.
        """
        if self._command:
            return os.path.basename(self._command[0])
        return "exec"

    def backup_targets(
        self, stem: str, output_dir: str, config: RuntimeConfig
    ) -> list[str]:
        """No backup targets.

        Args:
            stem: Input stem.
            output_dir: Output directory.
            config: Runtime config.

        Returns:
            Empty list.
        """
        return []
