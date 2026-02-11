"""Tests for cfour module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.cfour import CfourModule


class TestCfourModule:
    """Tests for CfourModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = CfourModule()
        assert mod.metadata.name == "cfour"
        assert mod.metadata.input_extensions == (".inp",)
        assert mod.metadata.uses_scratch is True
        assert mod.metadata.uses_archive is False

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = CfourModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_genbas(self, tmp_path: object) -> None:
        """Accepts -g flag with value."""
        mod = CfourModule()
        config = init_runtime_config(Defaults())
        genbas = tmp_path / "GENBAS"  # type: ignore[operator]
        genbas.write_text("basis data")
        mod.parse_args(["-g", str(genbas)], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = CfourModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips extension."""
        mod = CfourModule()
        assert mod.job_name("path/to/scf.inp") == "scf"

    def test_emit_run_command(self) -> None:
        """Run command references CFOUR, GENBAS, flock."""
        mod = CfourModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        sw = SoftwareConfig(paths={"cfour_dir": "/opt/cfour"})
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "CFOUR" in result
        assert "GENBAS" in result
        assert "flock" in result
        assert "xcfour" in result

    def test_emit_retrieve_outputs_empty(self) -> None:
        """Retrieve outputs is no-op."""
        mod = CfourModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        assert out.getvalue() == ""

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log, .tar.gz."""
        mod = CfourModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("test", "output/", config)
        assert "output/test.out" in targets
        assert "output/test.log" in targets
        assert "output/test.tar.gz" in targets

    def test_no_custom_build_jobs(self) -> None:
        """CFOUR does not override build_jobs."""
        mod = CfourModule()
        assert mod.has_custom_build_jobs is False
