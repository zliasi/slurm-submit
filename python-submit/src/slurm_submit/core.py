"""Core utilities: error handling, validation, path helpers."""

import logging
import os
import re
import sys

logger = logging.getLogger("slurm_submit")


class SubmitError(Exception):
    """Fatal error -- prints message to stderr, exits 1."""


class UsageError(SubmitError):
    """Bad usage -- prints message + hint to stderr, exits 1."""


def die(message: str) -> None:
    """Print error and raise SubmitError.

    Args:
        message: Error message to display.

    Raises:
        SubmitError: Always.
    """
    assert isinstance(message, str), "message must be a string"
    raise SubmitError(message)


def die_usage(message: str) -> None:
    """Print error with usage hint and raise UsageError.

    Args:
        message: Error message to display.

    Raises:
        UsageError: Always.
    """
    assert isinstance(message, str), "message must be a string"
    raise UsageError(message)


def program_invocation() -> str:
    """Return basename of current program invocation.

    Returns:
        Basename of sys.argv[0].
    """
    result = os.path.basename(sys.argv[0])
    assert isinstance(result, str), "invocation must be a string"
    return result


def validate_file_exists(filepath: str) -> None:
    """Validate that filepath exists as a regular file.

    Args:
        filepath: Path to validate.

    Raises:
        UsageError: If file not found.
    """
    assert isinstance(filepath, str), "filepath must be a string"
    if not os.path.isfile(filepath):
        die_usage(f"File not found: {filepath}")


def validate_positive_integer(value: str, param_name: str) -> None:
    """Validate value is a positive integer string.

    Args:
        value: Value to validate.
        param_name: Parameter name for error messages.

    Raises:
        UsageError: If invalid.
    """
    assert isinstance(value, str), "value must be a string"
    assert isinstance(param_name, str), "param_name must be a string"
    if not re.fullmatch(r"[1-9][0-9]*", value):
        die_usage(f"Invalid value for {param_name}: must be positive integer")


def validate_positive_number(value: str, param_name: str) -> None:
    """Validate value is a positive number string (int or float).

    Args:
        value: Value to validate.
        param_name: Parameter name for error messages.

    Raises:
        UsageError: If invalid.
    """
    assert isinstance(value, str), "value must be a string"
    assert isinstance(param_name, str), "param_name must be a string"
    if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", value) or value == "0":
        die_usage(f"Invalid value for {param_name}: must be positive number")


def validate_time_format(time_str: str) -> None:
    """Validate SLURM time format (D-HH:MM:SS or HH:MM:SS).

    Args:
        time_str: Time string to validate. Empty string is valid.

    Raises:
        UsageError: If invalid format.
    """
    assert isinstance(time_str, str), "time_str must be a string"
    if not time_str:
        return
    pattern = r"^([0-9]+-)?([0-1]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$"
    if not re.fullmatch(pattern, time_str):
        die_usage(f"Invalid time format: {time_str} (use D-HH:MM:SS)")


def validate_file_extension(filepath: str, allowed: tuple[str, ...]) -> None:
    """Validate that filepath has one of the allowed extensions.

    Args:
        filepath: File path to check.
        allowed: Tuple of allowed extensions (e.g. (".inp", ".xyz")).

    Raises:
        UsageError: If extension not allowed.
    """
    assert isinstance(filepath, str), "filepath must be a string"
    assert isinstance(allowed, tuple), "allowed must be a tuple"
    if not allowed:
        return
    for ext in allowed:
        if filepath.endswith(ext):
            return
    die_usage(f"Invalid extension for {filepath} (expected: {' '.join(allowed)})")


def to_absolute_path(filepath: str) -> str:
    """Convert a path to absolute.

    Args:
        filepath: Path to convert.

    Returns:
        Absolute path.
    """
    assert isinstance(filepath, str), "filepath must be a string"
    result = os.path.realpath(filepath)
    assert os.path.isabs(result), "result must be absolute"
    return result


def strip_extension(filepath: str, extension: str) -> str:
    """Strip extension from a filename's basename.

    Args:
        filepath: File path.
        extension: Extension to strip (e.g. ".inp").

    Returns:
        Basename without extension.
    """
    assert isinstance(filepath, str), "filepath must be a string"
    assert isinstance(extension, str), "extension must be a string"
    base = os.path.basename(filepath)
    if base.endswith(extension):
        return base[: -len(extension)]
    return base


def ensure_directory(dir_path: str) -> None:
    """Ensure directory exists, creating if needed.

    Args:
        dir_path: Directory path.
    """
    assert isinstance(dir_path, str), "dir_path must be a string"
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        logger.info("Created directory: %s", dir_path)


def normalize_output_dir(dir_path: str) -> str:
    """Normalize output directory path (ensure trailing slash).

    Args:
        dir_path: Directory path.

    Returns:
        Path with trailing slash.
    """
    assert isinstance(dir_path, str), "dir_path must be a string"
    if dir_path and not dir_path.endswith("/"):
        return dir_path + "/"
    return dir_path


def require_arg_value(flag: str, next_index: int, array_length: int) -> None:
    """Validate that a flag has a following value argument.

    Args:
        flag: The flag string.
        next_index: Index of the expected value.
        array_length: Total length of the args array.

    Raises:
        UsageError: If no value follows the flag.
    """
    assert isinstance(flag, str), "flag must be a string"
    assert next_index >= 0, "next_index must be non-negative"
    if next_index >= array_length:
        die_usage(f"Option {flag} requires a value")
