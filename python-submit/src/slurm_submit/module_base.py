"""Abstract base class for submit modules."""

from __future__ import annotations

import io
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slurm_submit.config import RuntimeConfig, SoftwareConfig


@dataclass(frozen=True)
class ModuleMetadata:
    """Static metadata describing a module's properties.

    Attributes:
        name: Module name (e.g. "orca").
        input_extensions: Accepted input extensions (e.g. (".inp",)).
        output_extensions: Extensions of output files to backup.
        retrieve_extensions: Extensions to retrieve from scratch.
        default_cpus: Default CPU cores per task.
        default_memory_gb: Default memory in GB.
        default_throttle: Default max concurrent array subjobs.
        default_output_dir: Default output directory.
        uses_scratch: Whether module uses scratch directory.
        uses_archive: Whether module creates tar archive.
        memory_unit: "gb" for integer, "gb_float" for float memory.
    """

    name: str
    input_extensions: tuple[str, ...] = ()
    output_extensions: tuple[str, ...] = ()
    retrieve_extensions: tuple[str, ...] = ()
    default_cpus: int = 1
    default_memory_gb: float = 2.0
    default_throttle: int = 5
    default_output_dir: str = "output"
    uses_scratch: bool = False
    uses_archive: bool = False
    memory_unit: str = "gb"


@dataclass
class RunContext:
    """Context for module run command emission.

    Bundles the parameters needed by emit_run_command to keep
    the method signature under the 5-parameter guideline.

    Attributes:
        out: Output buffer for script lines.
        input_ref: Input file path or shell variable reference.
        stem_ref: Stem string or shell variable reference.
        config: Runtime configuration.
        software: Software-specific config (paths, deps).
    """

    out: io.StringIO
    input_ref: str
    stem_ref: str
    config: RuntimeConfig
    software: SoftwareConfig


@dataclass
class ScriptContext:
    """All data needed to generate an sbatch script.

    Attributes:
        config: Runtime configuration.
        software: Software-specific config (paths, deps).
        metadata: Module metadata.
        module: The module instance.
        inputs: List of input entries (files or tab-separated job lines).
        job_name: Resolved job name.
        exec_manifest: Path to execution manifest (array mode).
        node_exclude: Comma-separated node exclude list.
    """

    config: RuntimeConfig
    software: SoftwareConfig
    metadata: ModuleMetadata
    module: SubmitModule
    inputs: list[str] = field(default_factory=list)
    job_name: str = ""
    exec_manifest: str = ""
    node_exclude: str = ""


class SubmitModule(ABC):
    """Abstract base for all submission modules.

    Subclasses must implement all abstract methods. Optional methods
    have default implementations that raise NotImplementedError; override
    detection uses ``has_custom_*`` properties.
    """

    @property
    @abstractmethod
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""

    @abstractmethod
    def print_usage(self) -> None:
        """Print module-specific usage text."""

    @abstractmethod
    def parse_args(self, args: list[str], config: RuntimeConfig) -> None:
        """Parse module-specific arguments.

        Args:
            args: Remaining args after common parsing.
            config: Runtime config (may be mutated by module flags).

        Raises:
            UsageError: On unknown or invalid args.
        """

    @abstractmethod
    def validate(self, config: RuntimeConfig) -> None:
        """Validate module state after parsing.

        Args:
            config: Runtime config.

        Raises:
            UsageError: On invalid state.
        """

    @abstractmethod
    def emit_dependencies(self, out: io.StringIO, software: SoftwareConfig) -> None:
        """Write environment setup lines (module load, exports).

        Args:
            out: Output buffer.
            software: Software config with deps string.
        """

    @abstractmethod
    def emit_run_command(self, ctx: RunContext) -> None:
        """Write execution command lines.

        Args:
            ctx: Run context with output buffer, input ref, stem ref,
                 runtime config, and software config.
        """

    @abstractmethod
    def emit_retrieve_outputs(
        self,
        out: io.StringIO,
        stem_ref: str,
        config: RuntimeConfig,
    ) -> None:
        """Write file retrieval lines for scratch outputs.

        Args:
            out: Output buffer.
            stem_ref: Stem string or shell variable reference.
            config: Runtime config.
        """

    @abstractmethod
    def job_name(self, input_file: str) -> str:
        """Compute job name from a single input file.

        Args:
            input_file: Input file path.

        Returns:
            Job name string (typically basename without extension).
        """

    @abstractmethod
    def backup_targets(
        self, stem: str, output_dir: str, config: RuntimeConfig
    ) -> list[str]:
        """List files to backup before submission.

        Args:
            stem: Input stem (basename without extension).
            output_dir: Output directory path.
            config: Runtime config.

        Returns:
            List of file paths to backup.
        """

    def build_jobs(
        self, positional_args: list[str], config: RuntimeConfig
    ) -> tuple[list[str], bool]:
        """Build job list from positional args (multi-file modules).

        Args:
            positional_args: Positional CLI arguments.
            config: Runtime config.

        Returns:
            Tuple of (inputs list, array_mode flag).

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    def create_exec_manifest(self, job_name: str) -> str:
        """Create execution manifest for array mode.

        Args:
            job_name: Job name for manifest filename.

        Returns:
            Path to created manifest file.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    def determine_job_name(self, config: RuntimeConfig) -> str:
        """Determine job name from internal job list.

        Args:
            config: Runtime config.

        Returns:
            Job name string.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    def backup_all(self, config: RuntimeConfig) -> None:
        """Backup all outputs for multi-file job list.

        Args:
            config: Runtime config.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    def generate_array_body(self, out: io.StringIO, ctx: ScriptContext) -> None:
        """Generate custom array job body.

        Args:
            out: Output buffer.
            ctx: Script generation context.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    def generate_single_body(self, out: io.StringIO, ctx: ScriptContext) -> None:
        """Generate custom single job body.

        Args:
            out: Output buffer.
            ctx: Script generation context.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    @property
    def has_custom_build_jobs(self) -> bool:
        """Whether this module overrides build_jobs."""
        return type(self).build_jobs is not SubmitModule.build_jobs

    @property
    def has_custom_create_exec_manifest(self) -> bool:
        """Whether this module overrides create_exec_manifest."""
        return type(self).create_exec_manifest is not SubmitModule.create_exec_manifest

    @property
    def has_custom_determine_job_name(self) -> bool:
        """Whether this module overrides determine_job_name."""
        return type(self).determine_job_name is not SubmitModule.determine_job_name

    @property
    def has_custom_backup_all(self) -> bool:
        """Whether this module overrides backup_all."""
        return type(self).backup_all is not SubmitModule.backup_all

    @property
    def has_custom_generate_array_body(self) -> bool:
        """Whether this module overrides generate_array_body."""
        return type(self).generate_array_body is not SubmitModule.generate_array_body

    @property
    def has_custom_generate_single_body(self) -> bool:
        """Whether this module overrides generate_single_body."""
        return type(self).generate_single_body is not SubmitModule.generate_single_body
