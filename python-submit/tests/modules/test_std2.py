"""Tests for std2 module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.std2 import Std2Module


class TestStd2Module:
    """Tests for Std2Module."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = Std2Module()
        assert mod.metadata.name == "std2"
        assert ".molden" in mod.metadata.input_extensions
        assert ".xyz" in mod.metadata.input_extensions
        assert mod.metadata.memory_unit == "gb_float"

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_ax(self) -> None:
        """Accepts -ax value."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.parse_args(["-ax", "0.30"], config)

    def test_parse_args_functional(self) -> None:
        """Functional preset sets ax value."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--B3LYP"], config)

    def test_parse_args_rejects_unknown_positional(self) -> None:
        """Unknown positional rejected."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown positional"):
            mod.parse_args(["badarg"], config)

    def test_validate_detects_molden_mode(self) -> None:
        """Validate detects molden mode from inputs."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.set_inputs(["mol.molden"])
        mod.validate(config)

    def test_validate_detects_xtb_mode(self) -> None:
        """Validate detects xtb mode from .xyz inputs."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.set_inputs(["mol.xyz"])
        mod.validate(config)

    def test_validate_rejects_mixed(self) -> None:
        """Cannot mix molden and xtb files."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.set_inputs(["mol.molden", "mol.xyz"])
        with pytest.raises(UsageError, match="Cannot mix"):
            mod.validate(config)

    def test_job_name(self) -> None:
        """Job name splits on first dot."""
        mod = Std2Module()
        assert mod.job_name("molecule.molden.inp") == "molecule"
        assert mod.job_name("test.xyz") == "test"

    def test_emit_run_command_molden(self) -> None:
        """Molden mode references std2 -f."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.set_inputs(["mol.molden"])
        mod.validate(config)
        out = io.StringIO()
        sw = SoftwareConfig(paths={"std2_exec": "/opt/std2"})
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "/opt/std2" in result
        assert "-f" in result

    def test_emit_run_command_xtb(self) -> None:
        """xTB mode references xtb4stda."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        mod.set_inputs(["mol.xyz"])
        mod.validate(config)
        out = io.StringIO()
        sw = SoftwareConfig(
            paths={"std2_exec": "/opt/std2", "xtb4stda_exec": "/opt/xtb4stda"}
        )
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "/opt/xtb4stda" in result
        assert "wfn.xtb" in result

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log, .tda.dat."""
        mod = Std2Module()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("mol", "output/", config)
        assert "output/mol.out" in targets
        assert "output/mol.log" in targets
        assert "output/mol.tda.dat" in targets

    def test_no_custom_build_jobs(self) -> None:
        """STD2 does not override build_jobs."""
        mod = Std2Module()
        assert mod.has_custom_build_jobs is False
