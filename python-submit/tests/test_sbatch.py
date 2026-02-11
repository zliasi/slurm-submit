"""Tests for sbatch script generation."""

from __future__ import annotations

import io
import os
from unittest.mock import patch

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.module_base import ScriptContext
from slurm_submit.modules.orca import OrcaModule
from slurm_submit.sbatch import (
    _submit_to_sbatch,
    _write_export,
    emit_archive_block,
    emit_job_footer,
    emit_sbatch_header,
    generate_sbatch_script,
    submit_job,
)


def _make_ctx(
    array_mode: bool = False,
    inputs: list[str] | None = None,
) -> ScriptContext:
    """Create a ScriptContext for testing.

    Args:
        array_mode: Whether array mode.
        inputs: Input list.

    Returns:
        ScriptContext.
    """
    config = init_runtime_config(Defaults())
    config.num_cpus = 4
    config.memory_gb = "8"
    config.output_dir = "output/"
    config.array_mode = array_mode
    config.throttle = 5

    module = OrcaModule()
    return ScriptContext(
        config=config,
        software=SoftwareConfig(
            paths={"orca_path": "/opt/orca"},
            dependencies="module purge",
        ),
        metadata=module.metadata,
        module=module,
        inputs=inputs or ["test.inp"],
        job_name="test",
        exec_manifest=".test.manifest" if array_mode else "",
        node_exclude="",
    )


class TestEmitSbatchHeader:
    """Tests for emit_sbatch_header."""

    def test_single_job_header(self) -> None:
        """Single job header has correct directives."""
        ctx = _make_ctx()
        out = io.StringIO()
        emit_sbatch_header(out, ctx)
        result = out.getvalue()
        assert "#!/bin/bash" in result
        assert "#SBATCH --job-name=test" in result
        assert "#SBATCH --cpus-per-task=4" in result
        assert "#SBATCH --mem=8gb" in result
        assert "#SBATCH --partition=chem" in result
        assert "#SBATCH --export=NONE" in result
        assert '--output="output/%x.log"' in result

    def test_array_job_header(self) -> None:
        """Array job header has array directive."""
        ctx = _make_ctx(array_mode=True, inputs=["a.inp", "b.inp", "c.inp"])
        out = io.StringIO()
        emit_sbatch_header(out, ctx)
        result = out.getvalue()
        assert '#SBATCH --output="/dev/null"' in result
        assert "#SBATCH --array=1-3%5" in result


class TestEmitJobFooter:
    """Tests for emit_job_footer."""

    def test_single_footer(self) -> None:
        """Single job footer uses SLURM_JOB_ID."""
        out = io.StringIO()
        emit_job_footer(out, False)
        result = out.getvalue()
        assert "SLURM_JOB_ID" in result
        assert "sacct" in result

    def test_array_footer(self) -> None:
        """Array job footer uses array IDs."""
        out = io.StringIO()
        emit_job_footer(out, True)
        result = out.getvalue()
        assert "SLURM_ARRAY_JOB_ID" in result


class TestEmitArchiveBlock:
    """Tests for emit_archive_block."""

    def test_archive_enabled(self) -> None:
        """Archive block creates tar.xz."""
        out = io.StringIO()
        emit_archive_block(out, True)
        assert "tar -cJf" in out.getvalue()

    def test_archive_disabled(self) -> None:
        """Disabled archive prints message."""
        out = io.StringIO()
        emit_archive_block(out, False)
        assert "Archive creation disabled" in out.getvalue()


class TestGenerateSbatchScript:
    """Tests for generate_sbatch_script."""

    def test_complete_script(self) -> None:
        """Complete script has all sections."""
        ctx = _make_ctx()
        script = generate_sbatch_script(ctx)
        assert "#!/bin/bash" in script
        assert "set -euo pipefail" in script
        assert "module purge" in script
        assert "orca" in script
        assert "sacct" in script


class TestWriteExport:
    """Tests for _write_export."""

    def test_writes_file(self, tmp_path: object) -> None:
        """Export writes script content to file."""
        filepath = str(tmp_path / "job.slurm")  # type: ignore[operator]
        result = _write_export("#!/bin/bash\necho hello\n", filepath)
        assert result == 0
        with open(filepath) as fh:
            assert "echo hello" in fh.read()
        assert os.access(filepath, os.X_OK)

    def test_returns_one_on_failure(self) -> None:
        """Returns 1 when write fails."""
        result = _write_export("content", "/nonexistent/path/job.slurm")
        assert result == 1


class TestSubmitToSbatch:
    """Tests for _submit_to_sbatch."""

    @patch("slurm_submit.sbatch.subprocess.run")
    def test_success(self, mock_run: object) -> None:
        """Returns 0 on successful sbatch submission."""
        mock_run.return_value.returncode = 0  # type: ignore[union-attr]
        ctx = _make_ctx()
        result = _submit_to_sbatch("#!/bin/bash\n", ctx)
        assert result == 0

    @patch("slurm_submit.sbatch.subprocess.run")
    def test_failure(self, mock_run: object) -> None:
        """Returns 1 on sbatch failure."""
        mock_run.return_value.returncode = 1  # type: ignore[union-attr]
        ctx = _make_ctx()
        result = _submit_to_sbatch("#!/bin/bash\n", ctx)
        assert result == 1


class TestSubmitJobExport:
    """Tests for submit_job with export mode."""

    def test_export_mode_writes_file(self, tmp_path: object) -> None:
        """submit_job with export_file writes to file instead of sbatch."""
        ctx = _make_ctx()
        filepath = str(tmp_path / "test.slurm")  # type: ignore[operator]
        ctx.config.export_file = filepath
        result = submit_job(ctx)
        assert result == 0
        assert os.path.isfile(filepath)

    @patch("slurm_submit.sbatch.subprocess.run")
    def test_normal_mode_calls_sbatch(self, mock_run: object) -> None:
        """submit_job without export_file pipes to sbatch."""
        mock_run.return_value.returncode = 0  # type: ignore[union-attr]
        ctx = _make_ctx()
        ctx.config.export_file = ""
        result = submit_job(ctx)
        assert result == 0
        mock_run.assert_called_once()  # type: ignore[union-attr]
