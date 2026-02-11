"""Tests for CLI entry point and module resolution."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from slurm_submit.cli import main, resolve_module
from slurm_submit.core import SubmitError, UsageError


class TestResolveModule:
    """Tests for resolve_module."""

    def test_resolve_module_with_prefix(self) -> None:
        """argv=['sorca', 'input.inp'] resolves to 'orca'."""
        name, invocation, remaining = resolve_module(["sorca", "input.inp"])
        assert name == "orca"
        assert invocation == "sorca"
        assert remaining == ["input.inp"]

    def test_resolve_module_submit_form(self) -> None:
        """argv=['submit', 'orca', 'input.inp'] resolves to 'orca'."""
        name, invocation, remaining = resolve_module(["submit", "orca", "input.inp"])
        assert name == "orca"
        assert invocation == "submit"
        assert remaining == ["input.inp"]

    def test_resolve_module_unknown_exits(self) -> None:
        """Unknown module triggers sys.exit."""
        with pytest.raises(SystemExit):
            resolve_module(["sunknown"])

    def test_resolve_module_submit_no_args_exits(self) -> None:
        """'submit' with no module arg triggers sys.exit."""
        with pytest.raises(SystemExit):
            resolve_module(["submit"])


class TestMain:
    """Tests for main error handling."""

    @patch("slurm_submit.cli._main_inner")
    def test_main_catches_usage_error(self, mock_inner: object) -> None:
        """UsageError returns exit code 1."""
        mock_inner.side_effect = UsageError("bad usage")  # type: ignore[union-attr]
        assert main() == 1

    @patch("slurm_submit.cli._main_inner")
    def test_main_catches_submit_error(self, mock_inner: object) -> None:
        """SubmitError returns exit code 1."""
        mock_inner.side_effect = SubmitError("failed")  # type: ignore[union-attr]
        assert main() == 1
