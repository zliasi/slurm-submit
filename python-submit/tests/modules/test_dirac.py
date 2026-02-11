"""Tests for dirac module."""

from __future__ import annotations

import pytest

from slurm_submit.config import Defaults, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.modules.dirac import DiracModule


class TestDiracModule:
    """Tests for DiracModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = DiracModule()
        assert mod.metadata.name == "dirac"
        assert mod.metadata.uses_scratch is True

    def test_has_custom_build_jobs(self) -> None:
        """Dirac overrides build_jobs."""
        mod = DiracModule()
        assert mod.has_custom_build_jobs is True

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips .inp extension."""
        mod = DiracModule()
        assert mod.job_name("path/to/sp.inp") == "sp"

    def test_build_jobs_single_pair(self, tmp_path: object) -> None:
        """Builds job from inp + mol pair."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        inp = tmp_path / "sp.inp"  # type: ignore[operator]
        mol = tmp_path / "h2o.mol"  # type: ignore[operator]
        inp.write_text("**DIRAC")
        mol.write_text("mol data")
        jobs, array_mode = mod.build_jobs([str(inp), str(mol)], config)
        assert len(jobs) == 1
        assert array_mode is False

    def test_build_jobs_multiple_pairs(self, tmp_path: object) -> None:
        """Multiple pairs create array job."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        inp1 = tmp_path / "sp1.inp"  # type: ignore[operator]
        mol1 = tmp_path / "h2o.mol"  # type: ignore[operator]
        inp2 = tmp_path / "sp2.inp"  # type: ignore[operator]
        mol2 = tmp_path / "nh3.mol"  # type: ignore[operator]
        inp1.write_text("inp1")
        mol1.write_text("mol1")
        inp2.write_text("inp2")
        mol2.write_text("mol2")
        jobs, array_mode = mod.build_jobs(
            [str(inp1), str(mol1), str(inp2), str(mol2)], config
        )
        assert len(jobs) == 2
        assert array_mode is True

    def test_build_jobs_odd_tokens(self, tmp_path: object) -> None:
        """Odd number of tokens rejected."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        inp = tmp_path / "sp.inp"  # type: ignore[operator]
        inp.write_text("inp")
        with pytest.raises(UsageError, match="requires both"):
            mod.build_jobs([str(inp)], config)

    def test_determine_job_name_single(self, tmp_path: object) -> None:
        """Single job name from inp_mol stem."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        inp = tmp_path / "sp.inp"  # type: ignore[operator]
        mol = tmp_path / "h2o.mol"  # type: ignore[operator]
        inp.write_text("inp")
        mol.write_text("mol")
        mod.build_jobs([str(inp), str(mol)], config)
        config.array_mode = False
        name = mod.determine_job_name(config)
        assert name == "sp_h2o"

    def test_determine_job_name_array(self, tmp_path: object) -> None:
        """Array job name format."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        inp1 = tmp_path / "sp1.inp"  # type: ignore[operator]
        mol1 = tmp_path / "h2o.mol"  # type: ignore[operator]
        inp2 = tmp_path / "sp2.inp"  # type: ignore[operator]
        mol2 = tmp_path / "nh3.mol"  # type: ignore[operator]
        inp1.write_text("inp1")
        mol1.write_text("mol1")
        inp2.write_text("inp2")
        mol2.write_text("mol2")
        mod.build_jobs([str(inp1), str(mol1), str(inp2), str(mol2)], config)
        config.array_mode = True
        config.throttle = 3
        name = mod.determine_job_name(config)
        assert name == "dirac-array-2t3"

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log."""
        mod = DiracModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("sp_h2o", "output/", config)
        assert "output/sp_h2o.out" in targets
        assert "output/sp_h2o.log" in targets
