"""Module: Python script submission.

Category B: passthrough args, multiple env managers, float memory.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from slurm_submit.core import die_usage, require_arg_value, strip_extension
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="python",
    input_extensions=(".py",),
    default_cpus=1,
    default_memory_gb=1.0,
    default_throttle=10,
    default_output_dir=".",
    uses_scratch=False,
    uses_archive=False,
    memory_unit="gb_float",
)


@register_module("python")
class PythonModule(SubmitModule):
    """Python script submission module."""

    def __init__(self) -> None:
        """Initialize module state."""
        self._python_exec = "python3"
        self._python_module = ""
        self._conda_env = ""
        self._venv_path = ""
        self._conda_activate = ""
        self._uv_enabled = False
        self._uv_project_path = ""
        self._script_args = ""

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " Python submission\n"
            "\n"
            " Environment options:\n"
            "   --python EXEC          Python executable (default: python3)\n"
            "   --python-module MOD    Activate conda module\n"
            "   --conda-env ENV        Activate conda environment\n"
            "   --venv PATH            Activate virtualenv\n"
            "   --conda-activate ENV   Source conda activate\n"
            "   --uv                   Use uv in current directory\n"
            "   --uv-project PATH      Use uv with project directory\n"
            "\n"
            " Script options:\n"
            '   --args "ARG1 ARG2"     Pass arguments to Python script\n'
            "\n"
            " Examples:\n"
            "   spython analysis.py -c 4 -m 8\n"
            '   spython script.py --conda-env myenv --args "--verbose"\n'
            "   spython *.py --uv -T 5\n"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Parse environment and script args.

        Args:
            args: Remaining args.
            config: Runtime config.
        """
        assert isinstance(args, list), "args must be a list"
        assert config is not None, "config must not be None"

        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "--python":
                require_arg_value(arg, i + 1, len(args))
                self._python_exec = args[i + 1]
                i += 2
            elif arg == "--python-module":
                require_arg_value(arg, i + 1, len(args))
                self._python_module = args[i + 1]
                i += 2
            elif arg == "--conda-env":
                require_arg_value(arg, i + 1, len(args))
                self._conda_env = args[i + 1]
                i += 2
            elif arg == "--venv":
                require_arg_value(arg, i + 1, len(args))
                self._venv_path = args[i + 1]
                i += 2
            elif arg == "--conda-activate":
                require_arg_value(arg, i + 1, len(args))
                self._conda_activate = args[i + 1]
                i += 2
            elif arg == "--uv":
                self._uv_enabled = True
                i += 1
            elif arg == "--uv-project":
                require_arg_value(arg, i + 1, len(args))
                self._uv_project_path = args[i + 1]
                self._uv_enabled = True
                i += 2
            elif arg == "--args":
                require_arg_value(arg, i + 1, len(args))
                self._script_args = args[i + 1]
                i += 2
            else:
                die_usage(f"Unknown option: {arg}")

    def validate(self, config: RuntimeConfig) -> None:
        """Validate uv project path if specified.

        Args:
            config: Runtime config.
        """
        if self._uv_enabled and self._uv_project_path:
            import os

            if not os.path.isdir(self._uv_project_path):
                die_usage(f"uv project path not found: {self._uv_project_path}")

    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """Emit environment setup.

        Args:
            out: Output buffer.
            software: Software config.
        """
        if software.dependencies:
            out.write(software.dependencies + "\n")

    def emit_run_command(self, ctx: RunContext) -> None:
        """Emit python run command with env setup.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.config.num_cpus >= 1, "num_cpus must be >= 1"
        assert ctx.config.output_dir, "output_dir must not be empty"

        ctx.out.write(f"export OMP_NUM_THREADS={ctx.config.num_cpus}\n")

        if self._python_module:
            ctx.out.write(f"module load {self._python_module}\n")
        elif self._conda_env:
            ctx.out.write(f"conda activate {self._conda_env}\n")
        elif self._venv_path:
            ctx.out.write(f"source {self._venv_path}/bin/activate\n")
        elif self._conda_activate:
            ctx.out.write(
                "source $(conda info --base)/etc/profile.d/conda.sh"
                f" && conda activate {self._conda_activate}\n"
            )

        ctx.out.write(f'\ncd "{ctx.config.output_dir}"\n')

        if self._uv_enabled:
            if self._uv_project_path:
                ctx.out.write(
                    f"uv run --project {self._uv_project_path}"
                    f' python "{ctx.input_ref}" {self._script_args} 2>&1\n'
                )
            else:
                ctx.out.write(
                    f'uv run python "{ctx.input_ref}"' f" {self._script_args} 2>&1\n"
                )
        else:
            ctx.out.write(
                f'{self._python_exec} "{ctx.input_ref}"' f" {self._script_args} 2>&1\n"
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
        return strip_extension(input_file, ".py")

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
        return [f"{output_dir}{stem}{config.log_extension}"]
