"""Tests for molpro module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.molpro import MolproModule


class TestMolproModule:
    """Tests for MolproModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = MolproModule()
        assert mod.metadata.name == "molpro"
        assert mod.metadata.input_extensions == (".inp",)
        assert mod.metadata.uses_scratch is False
        assert mod.metadata.uses_archive is False

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = MolproModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = MolproModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips .inp extension."""
        mod = MolproModule()
        assert mod.job_name("path/to/test.inp") == "test"

    def test_emit_run_command(self) -> None:
        """Run command references molpro and SLURM_CPUS_ON_NODE."""
        mod = MolproModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        sw = SoftwareConfig(paths={"molpro_exec": "/opt/molpro/molpro"})
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "/opt/molpro/molpro" in result
        assert "SLURM_CPUS_ON_NODE" in result

    def test_emit_retrieve_outputs_empty(self) -> None:
        """Retrieve outputs is no-op."""
        mod = MolproModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        assert out.getvalue() == ""

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log."""
        mod = MolproModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("test", "output/", config)
        assert "output/test.out" in targets
        assert "output/test.log" in targets

    def test_no_custom_build_jobs(self) -> None:
        """Molpro does not override build_jobs."""
        mod = MolproModule()
        assert mod.has_custom_build_jobs is False
