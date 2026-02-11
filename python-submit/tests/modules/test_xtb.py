"""Tests for xtb module."""

from __future__ import annotations

import io

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.module_base import RunContext
from slurm_submit.modules.xtb import XtbModule


class TestXtbModule:
    """Tests for XtbModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = XtbModule()
        assert mod.metadata.name == "xtb"
        assert mod.metadata.input_extensions == (".xyz",)
        assert mod.metadata.memory_unit == "gb_float"

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = XtbModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_passthrough(self) -> None:
        """Passthrough flags collected."""
        mod = XtbModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--opt", "--chrg", "0"], config)

    def test_parse_args_omp_threads(self) -> None:
        """Accepts --omp-threads."""
        mod = XtbModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--omp-threads", "4"], config)

    def test_job_name(self) -> None:
        """Job name strips .xyz extension."""
        mod = XtbModule()
        assert mod.job_name("path/to/mol.xyz") == "mol"

    def test_emit_run_command(self) -> None:
        """Run command references xtb and OMP."""
        mod = XtbModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--opt"], config)
        out = io.StringIO()
        sw = SoftwareConfig(paths={"xtb_exec": "/opt/xtb/xtb"})
        ctx = RunContext(
            out=out,
            input_ref="$input_file",
            stem_ref="$stem",
            config=config,
            software=sw,
        )
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "/opt/xtb/xtb" in result
        assert "OMP_NUM_THREADS" in result
        assert "--opt" in result

    def test_emit_retrieve_outputs(self) -> None:
        """Retrieve outputs references .xyz files."""
        mod = XtbModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        result = out.getvalue()
        assert ".xyz" in result

    def test_backup_targets(self) -> None:
        """Backup targets includes .log, .opt.xyz, .md.xyz."""
        mod = XtbModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("mol", "output/", config)
        assert "output/mol.log" in targets
        assert "output/mol.opt.xyz" in targets
        assert "output/mol.md.xyz" in targets

    def test_no_custom_build_jobs(self) -> None:
        """xTB does not override build_jobs."""
        mod = XtbModule()
        assert mod.has_custom_build_jobs is False
