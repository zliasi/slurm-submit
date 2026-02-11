"""Tests for configuration loading."""

from __future__ import annotations

import pytest

from slurm_submit.config import (
    Defaults,
    apply_module_defaults,
    init_runtime_config,
    load_defaults,
    load_software_config,
)


class TestLoadDefaults:
    """Tests for load_defaults."""

    def test_load_from_dir(self, config_dir: str) -> None:
        """Load defaults from test config dir."""
        defaults = load_defaults(config_dir)
        assert defaults.partition == "test"
        assert defaults.cpus == 2
        assert defaults.memory_gb == 4
        assert defaults.throttle == 3
        assert defaults.output_directory == "out"
        assert defaults.create_archive is False

    def test_missing_dir_returns_defaults(self, tmp_path: object) -> None:
        """Missing defaults.toml returns Defaults() with default values."""
        defaults = load_defaults(str(tmp_path))
        assert defaults.partition == "chem"
        assert defaults.cpus == 1


class TestLoadSoftwareConfig:
    """Tests for load_software_config."""

    def test_missing_config(self, config_dir: str) -> None:
        """Missing software config returns empty SoftwareConfig."""
        sw = load_software_config(config_dir, "nonexistent")
        assert sw.paths == {}
        assert sw.dependencies == ""

    def test_load_existing(self, config_dir: str, tmp_path: object) -> None:
        """Load existing software config."""
        import os

        sw_dir = os.path.join(config_dir, "software")
        with open(os.path.join(sw_dir, "test.toml"), "w") as fh:
            fh.write('[paths]\nexec = "/usr/bin/test"\n')
            fh.write('[dependencies]\nsetup = "module load test"\n')
        sw = load_software_config(config_dir, "test")
        assert sw.paths["exec"] == "/usr/bin/test"
        assert sw.dependencies == "module load test"

    def test_variant_loads_correct_file(self, config_dir: str) -> None:
        """Variant loads <module>-<variant>.toml."""
        import os

        sw_dir = os.path.join(config_dir, "software")
        with open(os.path.join(sw_dir, "orca-dev.toml"), "w") as fh:
            fh.write('[paths]\norca_path = "/opt/orca-dev"\n')
        sw = load_software_config(config_dir, "orca", variant="dev")
        assert sw.paths["orca_path"] == "/opt/orca-dev"

    def test_missing_variant_raises_usage_error(self, config_dir: str) -> None:
        """Missing variant file raises UsageError."""
        from slurm_submit.core import UsageError

        with pytest.raises(UsageError, match="Variant config not found"):
            load_software_config(config_dir, "orca", variant="nope")

    def test_empty_variant_loads_default(self, config_dir: str) -> None:
        """Empty variant string loads base config file."""
        import os

        sw_dir = os.path.join(config_dir, "software")
        with open(os.path.join(sw_dir, "orca.toml"), "w") as fh:
            fh.write('[paths]\norca_path = "/opt/orca"\n')
        sw = load_software_config(config_dir, "orca", variant="")
        assert sw.paths["orca_path"] == "/opt/orca"


class TestInitRuntimeConfig:
    """Tests for init_runtime_config."""

    def test_from_defaults(self) -> None:
        """RuntimeConfig mirrors Defaults."""
        defaults = Defaults(partition="kemi6", cpus=4, memory_gb=8)
        config = init_runtime_config(defaults)
        assert config.partition == "kemi6"
        assert config.num_cpus == 4
        assert config.memory_gb == "8"


class TestApplyModuleDefaults:
    """Tests for apply_module_defaults."""

    def test_overrides_config(self, default_config: object) -> None:
        """Module defaults override shipped defaults."""
        apply_module_defaults(default_config, 8, 16, 10, "results")  # type: ignore[arg-type]
        assert default_config.num_cpus == 8  # type: ignore[attr-defined]
        assert default_config.memory_gb == "16"  # type: ignore[attr-defined]
        assert default_config.throttle == 10  # type: ignore[attr-defined]
        assert default_config.output_dir == "results"  # type: ignore[attr-defined]
