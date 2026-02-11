"""Tests for partition module."""

from __future__ import annotations

from slurm_submit.config import RuntimeConfig
from slurm_submit.partition import setup_partition_specifics


class TestSetupPartitionSpecifics:
    """Tests for setup_partition_specifics."""

    def test_matching_partition(
        self, tmp_path: object, default_config: RuntimeConfig
    ) -> None:
        """Sets node_exclude when partition matches."""
        exclude_file = tmp_path / "excludes.txt"  # type: ignore[operator]
        exclude_file.write_text("node01\nnode02\n")
        default_config.partition = "chem"
        default_config.node_exclude_partition = "chem"
        default_config.node_exclude_file = str(exclude_file)
        setup_partition_specifics(default_config)
        assert default_config.node_exclude == "node01,node02"

    def test_different_partition(self, default_config: RuntimeConfig) -> None:
        """No excludes when partition doesn't match."""
        default_config.partition = "kemi6"
        default_config.node_exclude_partition = "chem"
        setup_partition_specifics(default_config)
        assert default_config.node_exclude == ""

    def test_missing_file(self, default_config: RuntimeConfig) -> None:
        """No excludes when file doesn't exist."""
        default_config.partition = "chem"
        default_config.node_exclude_partition = "chem"
        default_config.node_exclude_file = "/nonexistent"
        setup_partition_specifics(default_config)
        assert default_config.node_exclude == ""
