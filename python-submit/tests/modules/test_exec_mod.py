"""Tests for exec module."""

from __future__ import annotations

import io

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.module_base import RunContext
from slurm_submit.modules.exec_mod import ExecModule


class TestExecModule:
    """Tests for ExecModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = ExecModule()
        assert mod.metadata.name == "exec"
        assert mod.metadata.input_extensions == ()

    def test_parse_args_double_dash(self) -> None:
        """Command parsed after --."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--", "./myprogram", "arg1"], config)
        mod.validate(config)

    def test_parse_args_executable(self) -> None:
        """Command set via -x."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["-x", "./myprogram"], config)
        mod.validate(config)

    def test_parse_args_mpi(self) -> None:
        """MPI flag accepted."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--mpi", "--", "./prog"], config)
        mod.validate(config)

    def test_validate_no_command(self) -> None:
        """Validate fails without command."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)
        with pytest.raises(UsageError, match="No command"):
            mod.validate(config)

    def test_job_name_from_command(self) -> None:
        """Job name uses command basename."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--", "/opt/bin/myprogram"], config)
        assert mod.job_name("") == "myprogram"

    def test_job_name_default(self) -> None:
        """Job name defaults to exec."""
        mod = ExecModule()
        assert mod.job_name("") == "exec"

    def test_emit_run_command(self) -> None:
        """Run command emits the user command."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--", "./prog", "arg1"], config)
        out = io.StringIO()
        sw = SoftwareConfig()
        ctx = RunContext(out=out, input_ref="", stem_ref="", config=config, software=sw)
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "./prog" in result
        assert "EXIT_CODE" in result

    def test_emit_run_command_mpi(self) -> None:
        """MPI mode wraps with mpirun."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--mpi", "--", "./prog"], config)
        out = io.StringIO()
        sw = SoftwareConfig()
        ctx = RunContext(out=out, input_ref="", stem_ref="", config=config, software=sw)
        mod.emit_run_command(ctx)
        result = out.getvalue()
        assert "mpirun" in result

    def test_emit_retrieve_outputs_empty(self) -> None:
        """Retrieve outputs is no-op."""
        mod = ExecModule()
        out = io.StringIO()
        config = init_runtime_config(Defaults())
        mod.emit_retrieve_outputs(out, "", config)
        assert out.getvalue() == ""

    def test_backup_targets_empty(self) -> None:
        """Backup targets is empty."""
        mod = ExecModule()
        config = init_runtime_config(Defaults())
        assert mod.backup_targets("test", "output/", config) == []

    def test_no_custom_build_jobs(self) -> None:
        """Exec does not override build_jobs."""
        mod = ExecModule()
        assert mod.has_custom_build_jobs is False
