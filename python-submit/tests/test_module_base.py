"""Tests for module_base dataclasses and SubmitModule ABC."""

from __future__ import annotations

import io
from dataclasses import FrozenInstanceError
from typing import TYPE_CHECKING

import pytest

from slurm_submit.config import Defaults, SoftwareConfig, init_runtime_config
from slurm_submit.module_base import (
    ModuleMetadata,
    RunContext,
    ScriptContext,
    SubmitModule,
)

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig


class _MinimalModule(SubmitModule):
    """Concrete stub implementing only required abstract methods."""

    _meta = ModuleMetadata(name="stub")

    @property
    def metadata(self) -> ModuleMetadata:
        """Return stub metadata."""
        return self._meta

    def print_usage(self) -> None:
        """No-op."""

    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """No-op."""

    def validate(self, config: RuntimeConfig) -> None:
        """No-op."""

    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """No-op."""

    def emit_run_command(self, ctx: RunContext) -> None:
        """No-op."""

    def emit_retrieve_outputs(
        self, out: io.StringIO, stem_ref: str, config: RuntimeConfig
    ) -> None:
        """No-op."""

    def job_name(self, input_file: str) -> str:
        """Return fixed name."""
        return "stub-job"

    def backup_targets(
        self, stem: str, output_dir: str, config: RuntimeConfig
    ) -> list[str]:
        """Return empty list."""
        return []


class _OverrideModule(_MinimalModule):
    """Stub that overrides build_jobs."""

    def build_jobs(
        self, positional_args: list[str], config: RuntimeConfig
    ) -> tuple[list[str], bool]:
        """Return dummy jobs."""
        return positional_args, True


class TestModuleMetadata:
    """Tests for ModuleMetadata dataclass."""

    def test_metadata_defaults(self) -> None:
        """Default field values match expectations."""
        meta = ModuleMetadata(name="test")
        assert meta.name == "test"
        assert meta.input_extensions == ()
        assert meta.output_extensions == ()
        assert meta.retrieve_extensions == ()
        assert meta.default_cpus == 1
        assert meta.default_memory_gb == 2.0
        assert meta.default_throttle == 5
        assert meta.default_output_dir == "output"
        assert meta.uses_scratch is False
        assert meta.uses_archive is False
        assert meta.memory_unit == "gb"

    def test_metadata_frozen(self) -> None:
        """ModuleMetadata is immutable."""
        meta = ModuleMetadata(name="test")
        with pytest.raises(FrozenInstanceError):
            meta.name = "changed"  # type: ignore[misc]


class TestHasCustomDetection:
    """Tests for has_custom_* property detection."""

    def test_has_custom_detection_default(self) -> None:
        """Base impl returns False for all has_custom_* properties."""
        module = _MinimalModule()
        assert module.has_custom_build_jobs is False
        assert module.has_custom_create_exec_manifest is False
        assert module.has_custom_determine_job_name is False
        assert module.has_custom_backup_all is False
        assert module.has_custom_generate_array_body is False
        assert module.has_custom_generate_single_body is False

    def test_has_custom_detection_override(self) -> None:
        """Subclass overriding build_jobs is detected."""
        module = _OverrideModule()
        assert module.has_custom_build_jobs is True
        assert module.has_custom_create_exec_manifest is False


class TestRunContext:
    """Tests for RunContext dataclass."""

    def test_run_context_creation(self) -> None:
        """RunContext stores all fields."""
        config = init_runtime_config(Defaults())
        software = SoftwareConfig()
        out = io.StringIO()
        ctx = RunContext(
            out=out,
            input_ref="test.inp",
            stem_ref="test",
            config=config,
            software=software,
        )
        assert ctx.out is out
        assert ctx.input_ref == "test.inp"
        assert ctx.stem_ref == "test"
        assert ctx.config is config
        assert ctx.software is software


class TestScriptContext:
    """Tests for ScriptContext dataclass."""

    def test_script_context_creation(self) -> None:
        """ScriptContext stores all fields with defaults."""
        config = init_runtime_config(Defaults())
        software = SoftwareConfig()
        meta = ModuleMetadata(name="test")
        module = _MinimalModule()
        ctx = ScriptContext(
            config=config,
            software=software,
            metadata=meta,
            module=module,
        )
        assert ctx.config is config
        assert ctx.software is software
        assert ctx.metadata is meta
        assert ctx.module is module
        assert ctx.inputs == []
        assert ctx.job_name == ""
        assert ctx.exec_manifest == ""
        assert ctx.node_exclude == ""
