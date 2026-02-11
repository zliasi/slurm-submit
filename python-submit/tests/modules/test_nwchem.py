"""Tests for nwchem module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.nwchem import NWChemModule


class TestNWChemModule:
    """Tests for NWChemModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = NWChemModule()
        assert mod.metadata.name == "nwchem"
        assert mod.metadata.input_extensions == (".nw",)
        assert mod.metadata.uses_scratch is False
        assert mod.metadata.uses_archive is False

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = NWChemModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = NWChemModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips .nw extension."""
        mod = NWChemModule()
        assert mod.job_name("path/to/test.nw") == "test"

    def test_emit_run_command(self) -> None:
        """Run command references mpirun and nwchem."""
        mod = NWChemModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        sw = SoftwareConfig(paths={"nwchem_exec": "/opt/nwchem/nwchem"})
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "/opt/nwchem/nwchem" in result
        assert "mpirun" in result

    def test_emit_retrieve_outputs_empty(self) -> None:
        """Retrieve outputs is no-op."""
        mod = NWChemModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        assert out.getvalue() == ""

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log."""
        mod = NWChemModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("test", "output/", config)
        assert "output/test.out" in targets
        assert "output/test.log" in targets

    def test_no_custom_build_jobs(self) -> None:
        """NWChem does not override build_jobs."""
        mod = NWChemModule()
        assert mod.has_custom_build_jobs is False
