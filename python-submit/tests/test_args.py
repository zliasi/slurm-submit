"""Tests for argument parsing."""

from __future__ import annotations

import pytest

from slurm_submit.args import parse_common_args, validate_common_args
from slurm_submit.config import RuntimeConfig
from slurm_submit.core import UsageError


class TestParseCommonArgs:
    """Tests for parse_common_args."""

    def test_parse_cpus(self, default_config: RuntimeConfig) -> None:
        """Parse -c flag."""
        parsed = parse_common_args(["-c", "4"], default_config)
        assert default_config.num_cpus == 4
        assert parsed.positional_args == []

    def test_parse_memory(self, default_config: RuntimeConfig) -> None:
        """Parse -m flag."""
        parse_common_args(["-m", "8"], default_config)
        assert default_config.memory_gb == "8"

    def test_parse_partition(self, default_config: RuntimeConfig) -> None:
        """Parse -p flag."""
        parse_common_args(["-p", "kemi6"], default_config)
        assert default_config.partition == "kemi6"

    def test_parse_time(self, default_config: RuntimeConfig) -> None:
        """Parse -t flag."""
        parse_common_args(["-t", "1-12:00:00"], default_config)
        assert default_config.time_limit == "1-12:00:00"

    def test_parse_output(self, default_config: RuntimeConfig) -> None:
        """Parse -o flag."""
        parse_common_args(["-o", "results"], default_config)
        assert default_config.output_dir == "results"

    def test_parse_no_archive(self, default_config: RuntimeConfig) -> None:
        """Parse --no-archive flag."""
        parse_common_args(["--no-archive"], default_config)
        assert default_config.create_archive is False

    def test_positional_args(self, default_config: RuntimeConfig) -> None:
        """Collect positional args."""
        parsed = parse_common_args(
            ["file1.inp", "file2.inp", "-c", "4"], default_config
        )
        assert parsed.positional_args == ["file1.inp", "file2.inp"]

    def test_unknown_flags_to_remaining(self, default_config: RuntimeConfig) -> None:
        """Unknown flags go to remaining_args."""
        parsed = parse_common_args(["--unknown", "val", "-c", "4"], default_config)
        assert "--unknown" in parsed.remaining_args
        assert "val" in parsed.remaining_args

    def test_double_dash(self, default_config: RuntimeConfig) -> None:
        """-- sends rest to remaining_args."""
        parsed = parse_common_args(
            ["-c", "4", "--", "--opt", "file.xyz"], default_config
        )
        assert "--" in parsed.remaining_args
        assert "--opt" in parsed.remaining_args

    def test_parse_variant(self, default_config: RuntimeConfig) -> None:
        """Parse --variant flag."""
        parse_common_args(["--variant", "dev"], default_config)
        assert default_config.variant == "dev"

    def test_parse_export_with_value(self, default_config: RuntimeConfig) -> None:
        """Parse --export with explicit filename."""
        parse_common_args(["--export", "job.slurm"], default_config)
        assert default_config.export_file == "job.slurm"

    def test_parse_export_without_value(self, default_config: RuntimeConfig) -> None:
        """Parse --export without value sets sentinel."""
        from slurm_submit.args import _EXPORT_SENTINEL

        parse_common_args(["--export"], default_config)
        assert default_config.export_file == _EXPORT_SENTINEL

    def test_parse_export_before_flag(self, default_config: RuntimeConfig) -> None:
        """--export followed by another flag uses sentinel."""
        from slurm_submit.args import _EXPORT_SENTINEL

        parse_common_args(["--export", "-c", "4"], default_config)
        assert default_config.export_file == _EXPORT_SENTINEL
        assert default_config.num_cpus == 4

    def test_help_exits(self, default_config: RuntimeConfig) -> None:
        """--help causes SystemExit."""
        with pytest.raises(SystemExit):
            parse_common_args(["--help"], default_config)


class TestValidateCommonArgs:
    """Tests for validate_common_args."""

    def test_valid_config(self, default_config: RuntimeConfig) -> None:
        """Valid default config passes validation."""
        validate_common_args(default_config)

    def test_invalid_cpus(self, default_config: RuntimeConfig) -> None:
        """Non-integer cpus raises error."""
        default_config.num_cpus = 0
        with pytest.raises(UsageError):
            validate_common_args(default_config)

    def test_float_memory_mode(self, default_config: RuntimeConfig) -> None:
        """Float memory validated in gb_float mode."""
        default_config.memory_gb = "0.5"
        validate_common_args(default_config, "gb_float")

    def test_invalid_nice(self, default_config: RuntimeConfig) -> None:
        """Invalid nice factor raises error."""
        default_config.nice_factor = "abc"
        with pytest.raises(UsageError):
            validate_common_args(default_config)
