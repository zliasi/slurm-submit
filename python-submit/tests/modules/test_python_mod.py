"""Tests for python module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.python_mod import PythonModule


class TestPythonModule:
    """Tests for PythonModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = PythonModule()
        assert mod.metadata.name == "python"
        assert mod.metadata.input_extensions == (".py",)
        assert mod.metadata.uses_scratch is False
        assert mod.metadata.memory_unit == "gb_float"

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = PythonModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_uv(self) -> None:
        """Accepts --uv flag."""
        mod = PythonModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--uv"], config)

    def test_parse_args_conda_env(self) -> None:
        """Accepts --conda-env flag."""
        mod = PythonModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--conda-env", "myenv"], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = PythonModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips .py extension."""
        mod = PythonModule()
        assert mod.job_name("path/to/analysis.py") == "analysis"

    def test_emit_run_command_default(self) -> None:
        """Default run command uses python3."""
        mod = PythonModule()
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
        assert "python3" in result
        assert "OMP_NUM_THREADS" in result

    def test_emit_run_command_uv(self) -> None:
        """UV mode uses uv run python."""
        mod = PythonModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--uv"], config)
        out = io.StringIO()
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
        assert "uv run" in result

    def test_emit_run_command_conda(self) -> None:
        """Conda env mode activates env."""
        mod = PythonModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--conda-env", "myenv"], config)
        out = io.StringIO()
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
        assert "conda activate myenv" in result

    def test_emit_retrieve_outputs_empty(self) -> None:
        """Retrieve outputs is no-op."""
        mod = PythonModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "$stem", config)
        assert out.getvalue() == ""

    def test_backup_targets(self) -> None:
        """Backup targets is just .log."""
        mod = PythonModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("script", "output/", config)
        assert "output/script.log" in targets
        assert len(targets) == 1

    def test_no_custom_build_jobs(self) -> None:
        """Python does not override build_jobs."""
        mod = PythonModule()
        assert mod.has_custom_build_jobs is False
