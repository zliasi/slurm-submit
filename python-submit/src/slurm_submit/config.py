"""TOML configuration loading and layering.

Priority (low to high):
  1. config/defaults.toml (shipped defaults)
  2. Module metadata default_* fields
  3. config/software/<name>.toml (site-specific paths + deps)
  4. CLI arguments
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Defaults:
    """Shipped default values loaded from defaults.toml."""

    partition: str = "chem"
    cpus: int = 1
    memory_gb: int = 2
    ntasks: int = 1
    nodes: int = 1
    throttle: int = 5
    output_directory: str = "output"
    log_extension: str = ".log"
    use_backup_dir: bool = True
    max_backups: int = 5
    backup_dir_name: str = "backup"
    scratch_base: str = "/scratch"
    node_exclude_file: str = ""
    node_exclude_partition: str = "chem"
    create_archive: bool = True


@dataclass(frozen=True)
class SoftwareConfig:
    """Per-module software config loaded from config/software/<name>.toml."""

    paths: dict[str, str] = field(default_factory=dict)
    dependencies: str = ""


@dataclass
class RuntimeConfig:
    """Mutable runtime config accumulated from defaults + CLI args."""

    partition: str = "chem"
    num_cpus: int = 1
    memory_gb: str = "2"
    ntasks: int = 1
    nodes: int = 1
    throttle: int = 5
    output_dir: str = "output"
    log_extension: str = ".log"
    time_limit: str = ""
    custom_job_name: str = ""
    nice_factor: str = ""
    manifest_file: str = ""
    create_archive: bool = True
    array_mode: bool = False
    use_backup_dir: bool = True
    max_backups: int = 5
    backup_dir_name: str = "backup"
    scratch_base: str = "/scratch"
    node_exclude_file: str = ""
    node_exclude_partition: str = "chem"
    node_exclude: str = ""
    variant: str = ""
    export_file: str = ""


def find_config_dir() -> str:
    """Locate the config directory.

    Search order:
      1. SLURM_SUBMIT_CONFIG env var
      2. Relative to package source (../../config from this file)

    Returns:
        Path to config directory.

    Raises:
        FileNotFoundError: If config directory not found.
    """
    env_path = os.environ.get("SLURM_SUBMIT_CONFIG")
    if env_path and os.path.isdir(env_path):
        assert isinstance(env_path, str), "config path must be a string"
        assert os.path.isdir(env_path), "config path must be a directory"
        return env_path

    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(pkg_dir, "..", "..", "config"),
        os.path.join(pkg_dir, "config"),
    ]
    for candidate in candidates:
        resolved = os.path.realpath(candidate)
        if os.path.isdir(resolved):
            assert isinstance(resolved, str), "config path must be a string"
            assert os.path.isdir(resolved), "config path must be a directory"
            return resolved

    raise FileNotFoundError("Config directory not found")


def load_defaults(config_dir: str) -> Defaults:
    """Load defaults.toml from config_dir.

    Args:
        config_dir: Path to config directory.

    Returns:
        Defaults dataclass.
    """
    assert isinstance(config_dir, str), "config_dir must be a string"

    path = os.path.join(config_dir, "defaults.toml")
    if not os.path.isfile(path):
        return Defaults()

    with open(path, "rb") as fh:
        data = tomllib.load(fh)

    return Defaults(
        partition=_get(data, "partition", "default", "chem"),
        cpus=_get(data, "resources", "cpus", 1),
        memory_gb=_get(data, "resources", "memory_gb", 2),
        ntasks=_get(data, "resources", "ntasks", 1),
        nodes=_get(data, "resources", "nodes", 1),
        throttle=_get(data, "resources", "throttle", 5),
        output_directory=_get(data, "output", "directory", "output"),
        log_extension=_get(data, "output", "log_extension", ".log"),
        use_backup_dir=_get(data, "backup", "use_backup_dir", True),
        max_backups=_get(data, "backup", "max_backups", 5),
        backup_dir_name=_get(data, "backup", "dir_name", "backup"),
        scratch_base=_get(data, "scratch", "base", "/scratch"),
        node_exclude_file=_get(data, "node_exclude", "file", ""),
        node_exclude_partition=_get(data, "node_exclude", "partition", "chem"),
        create_archive=_get(data, "archive", "create", True),
    )


def load_software_config(
    config_dir: str,
    module_name: str,
    variant: str = "",
) -> SoftwareConfig:
    """Load config/software/<module_name>.toml or variant.

    When variant is non-empty, loads <module_name>-<variant>.toml instead.
    A missing variant file raises UsageError; a missing base file returns
    empty SoftwareConfig.

    Args:
        config_dir: Path to config directory.
        module_name: Module name (e.g. "orca").
        variant: Optional variant name (e.g. "dev").

    Returns:
        SoftwareConfig dataclass.

    Raises:
        UsageError: If variant file does not exist.
    """
    from slurm_submit.core import UsageError

    assert isinstance(config_dir, str), "config_dir must be a string"
    assert isinstance(module_name, str), "module_name must be a string"

    if variant:
        filename = f"{module_name}-{variant}.toml"
        path = os.path.join(config_dir, "software", filename)
        if not os.path.isfile(path):
            raise UsageError(f"Variant config not found: {filename}")
    else:
        path = os.path.join(config_dir, "software", f"{module_name}.toml")
        if not os.path.isfile(path):
            return SoftwareConfig()

    with open(path, "rb") as fh:
        data = tomllib.load(fh)

    paths = dict(data.get("paths", {}))
    deps_section = data.get("dependencies", {})
    deps = deps_section.get("setup", "") if isinstance(deps_section, dict) else ""

    return SoftwareConfig(paths=paths, dependencies=deps)


def init_runtime_config(defaults: Defaults) -> RuntimeConfig:
    """Initialize RuntimeConfig from Defaults.

    Args:
        defaults: Loaded defaults.

    Returns:
        Mutable RuntimeConfig.
    """
    assert isinstance(defaults, Defaults), "defaults must be a Defaults instance"

    return RuntimeConfig(
        partition=defaults.partition,
        num_cpus=defaults.cpus,
        memory_gb=str(defaults.memory_gb),
        ntasks=defaults.ntasks,
        nodes=defaults.nodes,
        throttle=defaults.throttle,
        output_dir=defaults.output_directory,
        log_extension=defaults.log_extension,
        create_archive=defaults.create_archive,
        use_backup_dir=defaults.use_backup_dir,
        max_backups=defaults.max_backups,
        backup_dir_name=defaults.backup_dir_name,
        scratch_base=defaults.scratch_base,
        node_exclude_file=defaults.node_exclude_file,
        node_exclude_partition=defaults.node_exclude_partition,
    )


def apply_module_defaults(
    config: RuntimeConfig,
    default_cpus: int,
    default_memory_gb: float,
    default_throttle: int,
    default_output_dir: str,
) -> None:
    """Apply module metadata defaults over shipped defaults.

    Args:
        config: RuntimeConfig to mutate.
        default_cpus: Module default CPU count.
        default_memory_gb: Module default memory.
        default_throttle: Module default throttle.
        default_output_dir: Module default output dir.
    """
    assert isinstance(config, RuntimeConfig), "config must be a RuntimeConfig"
    assert default_cpus > 0, "default_cpus must be positive"

    config.num_cpus = default_cpus
    config.memory_gb = str(default_memory_gb)
    config.throttle = default_throttle
    config.output_dir = default_output_dir


def _get(data: dict[str, Any], section: str, key: str, default: Any) -> Any:
    """Safely get a nested config value.

    Args:
        data: Parsed TOML dict.
        section: Top-level section name.
        key: Key within section.
        default: Fallback value.

    Returns:
        Config value or default.
    """
    assert isinstance(data, dict), "data must be a dict"
    assert isinstance(section, str), "section must be a string"
    assert isinstance(key, str), "key must be a string"

    return data.get(section, {}).get(key, default)
