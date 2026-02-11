"""Shared test fixtures."""

from __future__ import annotations

import pytest

from slurm_submit.config import (
    Defaults,
    RuntimeConfig,
    SoftwareConfig,
    init_runtime_config,
)


@pytest.fixture
def tmp_input(tmp_path: object) -> str:
    """Create a temporary .inp file.

    Args:
        tmp_path: Pytest tmp_path fixture.

    Returns:
        Path to temporary input file.
    """
    p = tmp_path / "test.inp"  # type: ignore[operator]
    p.write_text("test input")
    return str(p)


@pytest.fixture
def tmp_xyz(tmp_path: object) -> str:
    """Create a temporary .xyz file.

    Args:
        tmp_path: Pytest tmp_path fixture.

    Returns:
        Path to temporary xyz file.
    """
    p = tmp_path / "test.xyz"  # type: ignore[operator]
    p.write_text("2\n\nH 0 0 0\nH 0 0 1")
    return str(p)


@pytest.fixture
def default_config() -> RuntimeConfig:
    """Create a default RuntimeConfig.

    Returns:
        Default RuntimeConfig.
    """
    return init_runtime_config(Defaults())


@pytest.fixture
def default_software() -> SoftwareConfig:
    """Create a default SoftwareConfig.

    Returns:
        Default SoftwareConfig.
    """
    return SoftwareConfig()


@pytest.fixture
def config_dir(tmp_path: object) -> str:
    """Create a temporary config directory with defaults.toml.

    Args:
        tmp_path: Pytest tmp_path fixture.

    Returns:
        Path to config directory.
    """
    cfg = tmp_path / "config"  # type: ignore[operator]
    cfg.mkdir()
    (cfg / "software").mkdir()
    defaults = cfg / "defaults.toml"
    defaults.write_text(
        "[partition]\n"
        'default = "test"\n'
        "\n"
        "[resources]\n"
        "cpus = 2\n"
        "memory_gb = 4\n"
        "ntasks = 1\n"
        "nodes = 1\n"
        "throttle = 3\n"
        "\n"
        "[output]\n"
        'directory = "out"\n'
        'log_extension = ".log"\n'
        "\n"
        "[backup]\n"
        "use_backup_dir = true\n"
        "max_backups = 3\n"
        'dir_name = "bak"\n'
        "\n"
        "[scratch]\n"
        'base = "/tmp/scratch"\n'
        "\n"
        "[node_exclude]\n"
        'file = ""\n'
        'partition = "test"\n'
        "\n"
        "[archive]\n"
        "create = false\n"
    )
    return str(cfg)
