"""Tests for backup module."""

from __future__ import annotations

import io

from slurm_submit.backup import backup_existing_file, emit_backup_function_inline


class TestBackupExistingFile:
    """Tests for backup_existing_file."""

    def test_backup_creates_numbered_copy(self, tmp_path: object) -> None:
        """Backup creates .0 copy."""
        target = tmp_path / "test.out"  # type: ignore[operator]
        target.write_text("original")
        backup_existing_file(str(target), False, "backup", 5)
        assert not target.exists()
        assert (tmp_path / "test.out.0").exists()  # type: ignore[operator]

    def test_backup_rotates(self, tmp_path: object) -> None:
        """Multiple backups rotate correctly."""
        target = tmp_path / "test.out"  # type: ignore[operator]
        for i in range(3):
            target.write_text(f"version {i}")
            backup_existing_file(str(target), False, "backup", 5)
        assert (tmp_path / "test.out.0").exists()  # type: ignore[operator]
        assert (tmp_path / "test.out.1").exists()  # type: ignore[operator]
        assert (tmp_path / "test.out.2").exists()  # type: ignore[operator]

    def test_backup_with_dir(self, tmp_path: object) -> None:
        """Backup into subdirectory."""
        target = tmp_path / "test.out"  # type: ignore[operator]
        target.write_text("content")
        backup_existing_file(str(target), True, "bak", 3)
        bak_dir = tmp_path / "bak"  # type: ignore[operator]
        assert bak_dir.is_dir()
        assert (bak_dir / "test.out.0").exists()

    def test_backup_missing_file(self) -> None:
        """Backup of missing file is a no-op."""
        backup_existing_file("/nonexistent", True, "backup", 5)

    def test_backup_empty_path(self) -> None:
        """Backup of empty path is a no-op."""
        backup_existing_file("", True, "backup", 5)


class TestEmitBackupFunctionInline:
    """Tests for emit_backup_function_inline."""

    def test_emits_bash_function(self) -> None:
        """Emits valid bash function."""
        out = io.StringIO()
        emit_backup_function_inline(out, True, "backup", 5)
        result = out.getvalue()
        assert "backup_existing_files()" in result
        assert "flock" in result
        assert "mv -f" in result
