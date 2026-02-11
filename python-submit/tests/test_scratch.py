"""Tests for scratch directory setup and cleanup emission."""

from __future__ import annotations

import io

from slurm_submit.scratch import emit_scratch_cleanup, emit_scratch_setup


class TestScratchSetup:
    """Tests for emit_scratch_setup."""

    def test_scratch_setup_single_mode(self) -> None:
        """Single mode uses $SLURM_JOB_ID only."""
        out = io.StringIO()
        emit_scratch_setup(out, "/scratch", array_mode=False)
        text = out.getvalue()
        assert "$SLURM_JOB_ID" in text
        assert "$SLURM_ARRAY_TASK_ID" not in text

    def test_scratch_setup_array_mode(self) -> None:
        """Array mode uses $SLURM_ARRAY_TASK_ID."""
        out = io.StringIO()
        emit_scratch_setup(out, "/scratch", array_mode=True)
        text = out.getvalue()
        assert "$SLURM_ARRAY_TASK_ID" in text
        assert "$SLURM_JOB_ID" in text

    def test_scratch_setup_custom_base(self) -> None:
        """Custom scratch_base appears in output."""
        out = io.StringIO()
        emit_scratch_setup(out, "/tmp/my-scratch", array_mode=False)
        text = out.getvalue()
        assert "/tmp/my-scratch" in text
        assert "/scratch" not in text.replace("/tmp/my-scratch", "")

    def test_scratch_setup_creates_mkdir(self) -> None:
        """mkdir -p is emitted for scratch directory."""
        out = io.StringIO()
        emit_scratch_setup(out, "/scratch", array_mode=False)
        text = out.getvalue()
        assert 'mkdir -p "$scratch_directory"' in text


class TestScratchCleanup:
    """Tests for emit_scratch_cleanup."""

    def test_scratch_cleanup(self) -> None:
        """rm -rf command is emitted."""
        out = io.StringIO()
        emit_scratch_cleanup(out)
        text = out.getvalue()
        assert 'rm -rf "$scratch_directory"' in text
