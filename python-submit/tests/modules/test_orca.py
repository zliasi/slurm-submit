"""Tests for orca module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.orca import OrcaModule


class TestOrcaModule:
    """Tests for OrcaModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = OrcaModule()
        assert mod.metadata.name == "orca"
        assert mod.metadata.input_extensions == (".inp",)
        assert mod.metadata.uses_scratch is True
        assert mod.metadata.uses_archive is True

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = OrcaModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = OrcaModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips .inp extension."""
        mod = OrcaModule()
        assert mod.job_name("path/to/test.inp") == "test"

    def test_emit_run_command(self) -> None:
        """Run command references orca binary and scratch."""
        mod = OrcaModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        sw = SoftwareConfig(paths={"orca_path": "/opt/orca"})
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "/opt/orca/orca" in result
        assert "scratch_directory" in result

    def test_emit_retrieve_outputs(self) -> None:
        """Retrieve outputs references extensions."""
        mod = OrcaModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        result = out.getvalue()
        assert ".xyz" in result
        assert ".nto" in result
        assert ".cube" in result

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log, .tar.xz."""
        mod = OrcaModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("test", "output/", config)
        assert "output/test.out" in targets
        assert "output/test.log" in targets
        assert "output/test.tar.xz" in targets

    def test_no_custom_build_jobs(self) -> None:
        """Orca does not override build_jobs."""
        mod = OrcaModule()
        assert mod.has_custom_build_jobs is False
