"""Module: STD2 (simplified TD-DFT).

Category B: passthrough args, dual mode (molden vs xtb), float memory.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from slurm_submit.core import (
    die_usage,
    require_arg_value,
    validate_positive_number,
)
from slurm_submit.module_base import ModuleMetadata, RunContext, SubmitModule
from slurm_submit.modules import register_module

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig

logger = logging.getLogger("slurm_submit")

_META = ModuleMetadata(
    name="std2",
    input_extensions=(".molden", ".molden.inp", ".xyz", ".coord"),
    output_extensions=(".out",),
    default_cpus=1,
    default_memory_gb=0.5,
    default_throttle=10,
    default_output_dir=".",
    uses_scratch=False,
    uses_archive=False,
    memory_unit="gb_float",
)

_FUNCTIONAL_MAP: dict[str, str | None] = {
    "--PBE0": "0.25",
    "--B3LYP": "0.20",
    "--CAMB3LYP": None,
    "--wB97XD2": None,
    "--wB97XD3": None,
    "--wB97MV": None,
    "--SRC2R1": None,
    "--SRC2R2": None,
}


@register_module("std2")
class Std2Module(SubmitModule):
    """STD2 submission module."""

    def __init__(self) -> None:
        """Initialize module state."""
        self._std2_options: list[str] = []
        self._ax = "0.25"
        self._energy = "7.0"
        self._sty = "3"
        self._xtb_mode = False
        self._molden_mode = False
        self._use_spectrum = False
        self._inputs: list[str] = []

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return _META

    def print_usage(self) -> None:
        """Print module-specific usage."""
        logger.info(
            " STD2 submission (Molden + xTB modes)\n"
            "\n"
            " Molden options:\n"
            "   -ax FLOAT            Fock exchange (default: 0.25)\n"
            "   -e FLOAT             Energy threshold eV (default: 7.0)\n"
            "   -sty INT             Molden style (default: 3)\n"
            "   --PBE0, --B3LYP, --CAMB3LYP, --wB97XD2, --wB97XD3\n"
            "   --wB97MV, --SRC2R1, --SRC2R2\n"
            "   -rpa, -t, -vectm N, -nto N, -sf, -oldtda\n"
            "\n"
            " xTB mode (auto for .xyz/.coord):\n"
            "   -e FLOAT, -rpa\n"
            "\n"
            " Other:\n"
            "   --spectrum           Run g_spec after completion\n"
            "\n"
            " Examples:\n"
            "   sstd2 molecule.molden -ax 0.25 -e 10\n"
            "   sstd2 geometry.xyz -e 8 -rpa\n"
            "   sstd2 *.molden --PBE0 --throttle 5\n"
        )

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Parse module-specific + std2 passthrough args.

        Args:
            args: Remaining args.
            config: Runtime config.
        """
        assert isinstance(args, list), "args must be a list"
        assert config is not None, "config must not be None"

        self._std2_options = []
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "-ax":
                require_arg_value(arg, i + 1, len(args))
                self._ax = args[i + 1]
                i += 2
            elif arg == "-e":
                require_arg_value(arg, i + 1, len(args))
                self._energy = args[i + 1]
                i += 2
            elif arg == "-sty":
                require_arg_value(arg, i + 1, len(args))
                self._sty = args[i + 1]
                i += 2
            elif arg in _FUNCTIONAL_MAP:
                ax_val = _FUNCTIONAL_MAP[arg]
                if ax_val is not None:
                    self._ax = ax_val
                else:
                    self._std2_options.append(f"-{arg.lstrip('-')}")
                i += 1
            elif arg == "--spectrum":
                self._use_spectrum = True
                i += 1
            elif arg.startswith("-"):
                self._std2_options.append(arg)
                i += 1
            else:
                die_usage(f"Unknown positional arg: {arg}")

    def set_inputs(self, inputs: list[str]) -> None:
        """Store input list for mode detection in validate.

        Args:
            inputs: Resolved input file list.
        """
        self._inputs = inputs

    def validate(self, config: RuntimeConfig) -> None:
        """Validate options and detect molden/xtb mode.

        Args:
            config: Runtime config.
        """
        assert config is not None, "config must not be None"
        assert isinstance(self._inputs, list), "inputs must be a list"

        validate_positive_number(self._ax, "Fock exchange")
        validate_positive_number(self._energy, "energy threshold")

        self._molden_mode = False
        self._xtb_mode = False

        for input_file in self._inputs:
            if input_file.endswith((".molden", ".molden.inp")):
                self._molden_mode = True
            elif input_file.endswith((".xyz", ".coord")):
                self._xtb_mode = True

        if self._molden_mode and self._xtb_mode:
            die_usage("Cannot mix Molden and xTB files in same job")

    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """Emit environment setup.

        Args:
            out: Output buffer.
            software: Software config.
        """
        if software.dependencies:
            out.write(software.dependencies + "\n")

    def emit_run_command(self, ctx: RunContext) -> None:
        """Emit std2 run command (branches on molden vs xtb mode).

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """
        assert ctx.config.num_cpus >= 1, "num_cpus must be >= 1"
        assert ctx.config.output_dir, "output_dir must not be empty"

        std2_exec = ctx.software.paths.get("std2_exec", "std2")
        xtb4stda_exec = ctx.software.paths.get("xtb4stda_exec", "xtb4stda")
        opts_str = " ".join(self._std2_options) if self._std2_options else ""

        ctx.out.write(f"export OMP_NUM_THREADS={ctx.config.num_cpus}\n")
        ctx.out.write(f"export MKL_NUM_THREADS={ctx.config.num_cpus}\n")
        ctx.out.write(f'\ncd "{ctx.config.output_dir}"\n')

        if self._xtb_mode:
            ctx.out.write(
                f'{xtb4stda_exec} "{ctx.input_ref}"'
                f' > "{ctx.stem_ref}.xtb.out" 2>&1\n'
            )
            ctx.out.write("if [[ -f wfn.xtb ]]; then\n")
            ctx.out.write(
                f"  {std2_exec} -xtb -e {self._energy}"
                f" {opts_str}"
                f' > "{ctx.stem_ref}.out" 2>&1\n'
            )
            ctx.out.write(f'  mv wfn.xtb "{ctx.stem_ref}.wfn.xtb"\n')
            ctx.out.write("else\n")
            ctx.out.write('  printf "Error: xtb4stda failed to generate wfn.xtb\\n"\n')
            ctx.out.write("  exit 1\n")
            ctx.out.write("fi\n")
        else:
            ctx.out.write(
                f'{std2_exec} -f "{ctx.input_ref}" -sty {self._sty} \\\n'
                f"  -ax {self._ax} -e {self._energy}"
                f" {opts_str}"
                f' > "{ctx.stem_ref}.out" 2>&1\n'
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
        """Strip extension, handling multiple suffixes.

        Args:
            input_file: Input file path.

        Returns:
            Job name.
        """
        import os

        base = os.path.basename(input_file)
        return base.split(".")[0]

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
            f"{output_dir}{stem}.tda.dat",
        ]
