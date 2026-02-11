"""Tests for core utilities."""

from __future__ import annotations

import os

import pytest

from slurm_submit.core import (
    SubmitError,
    UsageError,
    die,
    die_usage,
    ensure_directory,
    normalize_output_dir,
    require_arg_value,
    strip_extension,
    validate_file_exists,
    validate_file_extension,
    validate_positive_integer,
    validate_positive_number,
    validate_time_format,
)


class TestDie:
    """Tests for die/die_usage."""

    def test_die_raises_submit_error(self) -> None:
        """Die raises SubmitError with message."""
        with pytest.raises(SubmitError, match="boom"):
            die("boom")

    def test_die_usage_raises_usage_error(self) -> None:
        """Die_usage raises UsageError with message."""
        with pytest.raises(UsageError, match="bad arg"):
            die_usage("bad arg")

    def test_usage_error_is_submit_error(self) -> None:
        """UsageError is a subclass of SubmitError."""
        assert issubclass(UsageError, SubmitError)


class TestValidateFileExists:
    """Tests for validate_file_exists."""

    def test_existing_file(self, tmp_input: str) -> None:
        """Existing file passes validation."""
        validate_file_exists(tmp_input)

    def test_missing_file(self) -> None:
        """Missing file raises UsageError."""
        with pytest.raises(UsageError, match="File not found"):
            validate_file_exists("/nonexistent/file.inp")


class TestValidatePositiveInteger:
    """Tests for validate_positive_integer."""

    def test_valid_integers(self) -> None:
        """Valid positive integers pass."""
        for val in ("1", "5", "100", "999"):
            validate_positive_integer(val, "test")

    def test_invalid_values(self) -> None:
        """Invalid values raise UsageError."""
        for val in ("0", "-1", "1.5", "abc", ""):
            with pytest.raises(UsageError):
                validate_positive_integer(val, "test")


class TestValidatePositiveNumber:
    """Tests for validate_positive_number."""

    def test_valid_numbers(self) -> None:
        """Valid positive numbers pass."""
        for val in ("1", "5", "1.5", "0.5", "100"):
            validate_positive_number(val, "test")

    def test_invalid_values(self) -> None:
        """Invalid values raise UsageError."""
        for val in ("0", "-1", "abc", ""):
            with pytest.raises(UsageError):
                validate_positive_number(val, "test")


class TestValidateTimeFormat:
    """Tests for validate_time_format."""

    def test_valid_formats(self) -> None:
        """Valid time formats pass."""
        validate_time_format("")
        validate_time_format("01:00:00")
        validate_time_format("23:59:59")
        validate_time_format("1-12:00:00")

    def test_invalid_formats(self) -> None:
        """Invalid time formats raise UsageError."""
        with pytest.raises(UsageError):
            validate_time_format("25:00:00")
        with pytest.raises(UsageError):
            validate_time_format("abc")


class TestValidateFileExtension:
    """Tests for validate_file_extension."""

    def test_valid_extension(self) -> None:
        """Matching extension passes."""
        validate_file_extension("test.inp", (".inp", ".xyz"))

    def test_invalid_extension(self) -> None:
        """Non-matching extension raises UsageError."""
        with pytest.raises(UsageError, match="Invalid extension"):
            validate_file_extension("test.txt", (".inp", ".xyz"))

    def test_empty_allowed(self) -> None:
        """Empty allowed tuple always passes."""
        validate_file_extension("test.txt", ())


class TestStripExtension:
    """Tests for strip_extension."""

    def test_strips_extension(self) -> None:
        """Strips matching extension."""
        assert strip_extension("path/to/test.inp", ".inp") == "test"

    def test_no_match(self) -> None:
        """Returns full basename if extension doesn't match."""
        assert strip_extension("test.xyz", ".inp") == "test.xyz"


class TestNormalizeOutputDir:
    """Tests for normalize_output_dir."""

    def test_adds_trailing_slash(self) -> None:
        """Adds trailing slash when missing."""
        assert normalize_output_dir("output") == "output/"

    def test_keeps_trailing_slash(self) -> None:
        """Keeps existing trailing slash."""
        assert normalize_output_dir("output/") == "output/"

    def test_empty_string(self) -> None:
        """Empty string stays empty."""
        assert normalize_output_dir("") == ""


class TestRequireArgValue:
    """Tests for require_arg_value."""

    def test_valid_index(self) -> None:
        """Valid index passes."""
        require_arg_value("-c", 1, 3)

    def test_out_of_bounds(self) -> None:
        """Out of bounds raises UsageError."""
        with pytest.raises(UsageError, match="requires a value"):
            require_arg_value("-c", 3, 3)


class TestEnsureDirectory:
    """Tests for ensure_directory."""

    def test_creates_directory(self, tmp_path: object) -> None:
        """Creates non-existent directory."""
        d = str(tmp_path) + "/newdir"  # type: ignore[operator]
        ensure_directory(d)
        assert os.path.isdir(d)

    def test_existing_directory(self, tmp_path: object) -> None:
        """Does nothing for existing directory."""
        ensure_directory(str(tmp_path))
