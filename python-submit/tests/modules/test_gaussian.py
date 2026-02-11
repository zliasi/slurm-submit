"""Tests for gaussian module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.gaussian import GaussianModule


class TestGaussianModule:
    """Tests for GaussianModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = GaussianModule()
        assert mod.metadata.name == "gaussian"
        assert mod.metadata.input_extensions == (".com", ".gjf")
        assert mod.metadata.uses_scratch is True
        assert mod.metadata.uses_archive is False

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = GaussianModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = GaussianModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name_com(self) -> None:
        """Job name strips .com extension."""
        mod = GaussianModule()
        assert mod.job_name("path/to/test.com") == "test"

    def test_job_name_gjf(self) -> None:
        """Job name strips .gjf extension."""
        mod = GaussianModule()
        assert mod.job_name("path/to/test.gjf") == "test"

    def test_emit_run_command(self) -> None:
        """Run command references gaussian exec and scratch."""
        mod = GaussianModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        sw = SoftwareConfig(paths={"gaussian_exec": "/opt/g16/g16"})
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "/opt/g16/g16" in result
        assert "GAUSS_SCRDIR" in result

    def test_emit_retrieve_outputs(self) -> None:
        """Retrieve outputs references .chk."""
        mod = GaussianModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        result = out.getvalue()
        assert ".chk" in result

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log, .chk."""
        mod = GaussianModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("test", "output/", config)
        assert "output/test.out" in targets
        assert "output/test.log" in targets
        assert "output/test.chk" in targets

    def test_no_custom_build_jobs(self) -> None:
        """Gaussian does not override build_jobs."""
        mod = GaussianModule()
        assert mod.has_custom_build_jobs is False
