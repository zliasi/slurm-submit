"""Partition-specific settings (node excludes)."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig

logger = logging.getLogger("slurm_submit")


def setup_partition_specifics(config: RuntimeConfig) -> None:
    """Configure partition-specific settings.

    Sets config.node_exclude from exclude file when partition matches.

    Args:
        config: RuntimeConfig to mutate.
    """
    assert hasattr(config, "partition"), "config must have partition attribute"
    assert hasattr(config, "node_exclude_file"), "config must have node_exclude_file"

    config.node_exclude = ""
    if (
        config.partition == config.node_exclude_partition
        and config.node_exclude_file
        and os.path.isfile(config.node_exclude_file)
    ):
        with open(config.node_exclude_file) as fh:
            nodes = [line.strip() for line in fh if line.strip()]
        config.node_exclude = ",".join(nodes)

    assert isinstance(config.node_exclude, str), "node_exclude must be a string"
