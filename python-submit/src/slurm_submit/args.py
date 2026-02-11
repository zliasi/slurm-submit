"""Common CLI argument parsing.

Parses flags shared by all modules. Unknown flags are collected for
module-specific parsing via module.parse_args().
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from slurm_submit.core import require_arg_value

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig

logger = logging.getLogger("slurm_submit")

COMMON_OPTIONS_TEXT = """\
 Common options:
   -c, --cpus INT               CPU cores per task
   -m, --memory NUM             Total memory in GB
   -p, --partition NAME         Partition
   -t, --time D-HH:MM:SS       Time limit
   -o, --output DIR             Output directory
   -M, --manifest FILE          Manifest file (job array)
   -T, --throttle INT           Max concurrent array subjobs
   -N, --nodes INT              Number of nodes
   -n, --ntasks INT             Number of tasks
   -j, --job, --job-name NAME   Custom job name
   -y, --nice INT               SLURM nice factor
   --variant NAME               Software variant (loads <module>-NAME.toml)
   --export [FILE]              Write sbatch script to FILE instead of submitting
   --no-archive                 Disable archive creation
   -h, --help                   Show this help"""


@dataclass
class ParsedArgs:
    """Result of common argument parsing.

    Attributes:
        positional_args: Non-flag arguments (input files).
        remaining_args: Unknown flags forwarded to module.
    """

    positional_args: list[str] = field(default_factory=list)
    remaining_args: list[str] = field(default_factory=list)


def print_common_options() -> None:
    """Print common option help lines."""
    logger.info(COMMON_OPTIONS_TEXT)


_VALUE_FLAGS: dict[str, str] = {
    "-o": "output_dir",
    "--output": "output_dir",
    "-c": "num_cpus",
    "--cpus": "num_cpus",
    "--cpu": "num_cpus",
    "-m": "memory_gb",
    "--memory": "memory_gb",
    "--mem": "memory_gb",
    "-p": "partition",
    "--partition": "partition",
    "-t": "time_limit",
    "--time": "time_limit",
    "-M": "manifest_file",
    "--manifest": "manifest_file",
    "-T": "throttle",
    "--throttle": "throttle",
    "-N": "nodes",
    "--nodes": "nodes",
    "-n": "ntasks",
    "--ntasks": "ntasks",
    "-j": "custom_job_name",
    "--job": "custom_job_name",
    "--job-name": "custom_job_name",
    "-y": "nice_factor",
    "--nice": "nice_factor",
    "--variant": "variant",
}

_EXPORT_SENTINEL = ":default:"

_INT_FIELDS = frozenset({"num_cpus", "throttle", "nodes", "ntasks"})


def _apply_value_flag(config: RuntimeConfig, attr: str, value: str) -> None:
    """Apply a parsed value flag to config.

    Args:
        config: RuntimeConfig to mutate.
        attr: Attribute name on config.
        value: String value from CLI.
    """
    assert hasattr(config, attr), f"config missing attribute: {attr}"
    if attr in _INT_FIELDS:
        setattr(config, attr, int(value))
    else:
        setattr(config, attr, value)


def _handle_unknown_flag(args: list[str], index: int, result: ParsedArgs) -> int:
    """Handle an unknown flag, collecting it for module parsing.

    Args:
        args: Full argument list.
        index: Current index.
        result: ParsedArgs to append to.

    Returns:
        New index after consuming the flag (and optional value).
    """
    result.remaining_args.append(args[index])
    if index + 1 < len(args) and not args[index + 1].startswith("-"):
        result.remaining_args.append(args[index + 1])
        return index + 2
    return index + 1


def parse_common_args(
    argv: list[str],
    config: RuntimeConfig,
    print_usage_fn: object = None,
) -> ParsedArgs:
    """Parse common CLI arguments, mutating config.

    Args:
        argv: Command line arguments to parse.
        config: RuntimeConfig to mutate with parsed values.
        print_usage_fn: Callable to print full usage (for -h/--help).

    Returns:
        ParsedArgs with positional and remaining args.
    """
    assert isinstance(argv, list), "argv must be a list"
    result = ParsedArgs()
    args = list(argv)
    length = len(args)
    i = 0

    while i < length:
        arg = args[i]

        if arg in _VALUE_FLAGS:
            require_arg_value(arg, i + 1, length)
            _apply_value_flag(config, _VALUE_FLAGS[arg], args[i + 1])
            i += 2
        elif arg == "--export":
            if i + 1 < length and not args[i + 1].startswith("-"):
                config.export_file = args[i + 1]
                i += 2
            else:
                config.export_file = _EXPORT_SENTINEL
                i += 1
        elif arg == "--no-archive":
            config.create_archive = False
            i += 1
        elif arg in ("-h", "--help"):
            if callable(print_usage_fn):
                print_usage_fn()
            sys.exit(0)
        elif arg == "--":
            result.remaining_args.extend(args[i:])
            break
        elif arg.startswith("-"):
            i = _handle_unknown_flag(args, i, result)
        else:
            result.positional_args.append(arg)
            i += 1

    assert isinstance(result, ParsedArgs), "result must be ParsedArgs"
    return result


def validate_common_args(config: RuntimeConfig, memory_unit: str = "gb") -> None:
    """Validate common arguments after parsing.

    Args:
        config: RuntimeConfig with parsed values.
        memory_unit: "gb" for integer validation, "gb_float" for float.

    Raises:
        UsageError: On invalid values.
    """
    from slurm_submit.core import (
        validate_positive_integer,
        validate_positive_number,
        validate_time_format,
    )

    assert memory_unit in ("gb", "gb_float"), "invalid memory_unit"
    validate_positive_integer(str(config.num_cpus), "CPU cores")

    if memory_unit == "gb_float":
        validate_positive_number(config.memory_gb, "memory")
    else:
        validate_positive_integer(config.memory_gb, "memory")

    validate_positive_integer(str(config.ntasks), "ntasks")
    validate_positive_integer(str(config.nodes), "nodes")
    validate_positive_integer(str(config.throttle), "throttle")

    if config.nice_factor:
        validate_positive_integer(config.nice_factor, "nice factor")

    validate_time_format(config.time_limit)
