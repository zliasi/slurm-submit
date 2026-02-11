"""Tests for manifest module."""

from __future__ import annotations

import os

import pytest

from slurm_submit.config import RuntimeConfig
from slurm_submit.core import UsageError
from slurm_submit.manifest import (
    create_manifest,
    default_backup_targets,
    default_job_name,
    resolve_inputs,
)


class TestResolveInputs:
    """Tests for resolve_inputs."""

    def test_single_file(self, tmp_input: str, default_config: RuntimeConfig) -> None:
        """Single positional arg -> single mode."""
        inputs, array_mode = resolve_inputs(default_config, [tmp_input], (".inp",))
        assert len(inputs) == 1
        assert array_mode is False

    def test_multiple_files(
        self, tmp_path: object, default_config: RuntimeConfig
    ) -> None:
        """Multiple positional args -> array mode."""
        files = []
        for i in range(3):
            p = tmp_path / f"test{i}.inp"  # type: ignore[operator]
            p.write_text(f"input {i}")
            files.append(str(p))
        inputs, array_mode = resolve_inputs(default_config, files, (".inp",))
        assert len(inputs) == 3
        assert array_mode is True

    def test_no_inputs(self, default_config: RuntimeConfig) -> None:
        """No inputs raises UsageError."""
        with pytest.raises(UsageError, match="No input files"):
            resolve_inputs(default_config, [], (".inp",))

    def test_missing_file(self, default_config: RuntimeConfig) -> None:
        """Missing file raises UsageError."""
        with pytest.raises(UsageError, match="File not found"):
            resolve_inputs(default_config, ["/nonexistent.inp"], (".inp",))

    def test_bad_extension(
        self, tmp_path: object, default_config: RuntimeConfig
    ) -> None:
        """Wrong extension raises UsageError."""
        p = tmp_path / "test.txt"  # type: ignore[operator]
        p.write_text("content")
        with pytest.raises(UsageError, match="Invalid extension"):
            resolve_inputs(default_config, [str(p)], (".inp",))


class TestCreateManifest:
    """Tests for create_manifest."""

    def test_creates_manifest(self, tmp_path: object) -> None:
        """Creates manifest with absolute paths."""
        os.chdir(str(tmp_path))
        p = tmp_path / "test.inp"  # type: ignore[operator]
        p.write_text("input")
        manifest = create_manifest([str(p)], "test", "")
        assert os.path.isfile(manifest)
        assert manifest == ".test.manifest"

    def test_custom_name(self, tmp_path: object) -> None:
        """Uses custom job name as manifest name."""
        os.chdir(str(tmp_path))
        p = tmp_path / "test.inp"  # type: ignore[operator]
        p.write_text("input")
        manifest = create_manifest([str(p)], "test", "custom")
        assert manifest == "custom"


class TestDefaultJobName:
    """Tests for default_job_name."""

    def test_strips_extension(self) -> None:
        """Strips matching extension."""
        assert default_job_name("path/test.inp", (".inp",)) == "test"

    def test_no_match(self) -> None:
        """Returns basename if no extension match."""
        assert default_job_name("path/test.xyz", (".inp",)) == "test.xyz"


class TestDefaultBackupTargets:
    """Tests for default_backup_targets."""

    def test_generates_targets(self) -> None:
        """Generates correct backup targets."""
        targets = default_backup_targets("test", "output/", (".out",), ".log", True)
        assert "output/test.out" in targets
        assert "output/test.log" in targets
        assert "output/test.tar.xz" in targets

    def test_no_archive(self) -> None:
        """Skips archive when disabled."""
        targets = default_backup_targets("test", "output/", (".out",), ".log", False)
        assert "output/test.tar.xz" not in targets
