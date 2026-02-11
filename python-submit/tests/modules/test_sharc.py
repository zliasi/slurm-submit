"""Tests for sharc module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.sharc import SharcModule


class TestSharcModule:
    """Tests for SharcModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = SharcModule()
        assert mod.metadata.name == "sharc"
        assert mod.metadata.input_extensions == (".inp",)
        assert mod.metadata.uses_scratch is True
        assert mod.metadata.uses_archive is True

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = SharcModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = SharcModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips .inp extension."""
        mod = SharcModule()
        assert mod.job_name("path/to/dynamics.inp") == "dynamics"

    def test_emit_run_command(self) -> None:
        """Run command references INITCONDS, QM, sharc.x."""
        mod = SharcModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        sw = SoftwareConfig()
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "INITCONDS" in result
        assert "QM" in result
        assert "sharc.x" in result
        assert "scratch_directory" in result

    def test_emit_retrieve_outputs(self) -> None:
        """Retrieve outputs references output.dat, output.log, restart."""
        mod = SharcModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        result = out.getvalue()
        assert "output.dat" in result
        assert "output.log" in result
        assert "restart" in result

    def test_backup_targets_with_archive(self) -> None:
        """Backup targets includes .tar.gz when archive enabled."""
        mod = SharcModule()
        config = init_runtime_config(Defaults())
        config.create_archive = True
        targets = mod.backup_targets("dyn", "output/", config)
        assert "output/dyn_output.dat" in targets
        assert "output/dyn_output.log" in targets
        assert "output/dyn.tar.gz" in targets

    def test_backup_targets_without_archive(self) -> None:
        """Backup targets excludes .tar.gz when archive disabled."""
        mod = SharcModule()
        config = init_runtime_config(Defaults())
        config.create_archive = False
        targets = mod.backup_targets("dyn", "output/", config)
        assert not any(t.endswith(".tar.gz") for t in targets)

    def test_no_custom_build_jobs(self) -> None:
        """SHARC does not override build_jobs."""
        mod = SharcModule()
        assert mod.has_custom_build_jobs is False
