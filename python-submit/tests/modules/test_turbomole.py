"""Tests for turbomole module."""

from __future__ import annotations

import pytest

from slurm_submit.config import Defaults, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.modules.turbomole import TurbomoleModule


class TestTurbomoleModule:
    """Tests for TurbomoleModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = TurbomoleModule()
        assert mod.metadata.name == "turbomole"
        assert mod.metadata.uses_scratch is False

    def test_has_custom_build_jobs(self) -> None:
        """Turbomole overrides build_jobs."""
        mod = TurbomoleModule()
        assert mod.has_custom_build_jobs is True

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips extension from basename."""
        mod = TurbomoleModule()
        assert mod.job_name("dft_opt/control") == "control"

    def test_build_jobs_single_pair(self, tmp_path: object) -> None:
        """Builds job from control + coord pair."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        control = tmp_path / "control"  # type: ignore[operator]
        coord = tmp_path / "coord"  # type: ignore[operator]
        control.write_text("$title test")
        coord.write_text("$coord")
        jobs, array_mode = mod.build_jobs([str(control), str(coord)], config)
        assert len(jobs) == 1
        assert array_mode is False

    def test_build_jobs_multiple_pairs(self, tmp_path: object) -> None:
        """Multiple pairs create array job."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        d1 = tmp_path / "job1"  # type: ignore[operator]
        d2 = tmp_path / "job2"  # type: ignore[operator]
        d1.mkdir()
        d2.mkdir()
        (d1 / "control").write_text("ctrl1")
        (d1 / "coord").write_text("coord1")
        (d2 / "control").write_text("ctrl2")
        (d2 / "coord").write_text("coord2")
        jobs, array_mode = mod.build_jobs(
            [
                str(d1 / "control"),
                str(d1 / "coord"),
                str(d2 / "control"),
                str(d2 / "coord"),
            ],
            config,
        )
        assert len(jobs) == 2
        assert array_mode is True

    def test_build_jobs_coord_without_control(self, tmp_path: object) -> None:
        """Coord without control rejected."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        coord = tmp_path / "coord"  # type: ignore[operator]
        coord.write_text("$coord")
        with pytest.raises(UsageError, match="without preceding control"):
            mod.build_jobs([str(coord)], config)

    def test_determine_job_name_single(self, tmp_path: object) -> None:
        """Single job name from control stem."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        control = tmp_path / "control"  # type: ignore[operator]
        coord = tmp_path / "coord"  # type: ignore[operator]
        control.write_text("ctrl")
        coord.write_text("coord")
        mod.build_jobs([str(control), str(coord)], config)
        config.array_mode = False
        name = mod.determine_job_name(config)
        assert name == "control"

    def test_determine_job_name_array(self, tmp_path: object) -> None:
        """Array job name format."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        d1 = tmp_path / "j1"  # type: ignore[operator]
        d2 = tmp_path / "j2"  # type: ignore[operator]
        d1.mkdir()
        d2.mkdir()
        (d1 / "control").write_text("c1")
        (d1 / "coord").write_text("c1")
        (d2 / "control").write_text("c2")
        (d2 / "coord").write_text("c2")
        mod.build_jobs(
            [
                str(d1 / "control"),
                str(d1 / "coord"),
                str(d2 / "control"),
                str(d2 / "coord"),
            ],
            config,
        )
        config.array_mode = True
        config.throttle = 10
        name = mod.determine_job_name(config)
        assert name == "turbomole-array-2t10"

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log."""
        mod = TurbomoleModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("control", "output/", config)
        assert "output/control.out" in targets
        assert "output/control.log" in targets
