"""Tests for dalton module."""

from __future__ import annotations

import pytest

from slurm_submit.config import Defaults, init_runtime_config
from slurm_submit.core import UsageError
from slurm_submit.modules.dalton import DaltonModule


class TestDaltonModule:
    """Tests for DaltonModule."""

    def test_metadata(self) -> None:
        """Module has correct metadata."""
        mod = DaltonModule()
        assert mod.metadata.name == "dalton"
        assert mod.metadata.uses_scratch is True

    def test_has_custom_build_jobs(self) -> None:
        """Dalton overrides build_jobs."""
        mod = DaltonModule()
        assert mod.has_custom_build_jobs is True

    def test_parse_args_empty(self) -> None:
        """Empty args accepted."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        mod.parse_args([], config)

    def test_parse_args_loprop(self) -> None:
        """Accepts --loprop flag."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        mod.parse_args(["--loprop"], config)

    def test_parse_args_rejects_unknown(self) -> None:
        """Unknown args rejected."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        with pytest.raises(UsageError, match="Unknown option"):
            mod.parse_args(["--bad"], config)

    def test_job_name(self) -> None:
        """Job name strips .dal extension."""
        mod = DaltonModule()
        assert mod.job_name("path/to/sp.dal") == "sp"

    def test_build_jobs_dal_mol(self, tmp_path: object) -> None:
        """Builds job from dal + mol pair."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        dal = tmp_path / "sp.dal"  # type: ignore[operator]
        mol = tmp_path / "h2o.mol"  # type: ignore[operator]
        dal.write_text("**DALTON\n.RUN WAVE")
        mol.write_text("ATOMBASIS\nmol data")
        jobs, array_mode = mod.build_jobs([str(dal), str(mol)], config)
        assert len(jobs) == 1
        assert array_mode is False

    def test_build_jobs_multiple_mol(self, tmp_path: object) -> None:
        """Multiple mol files create array job."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        dal = tmp_path / "sp.dal"  # type: ignore[operator]
        mol1 = tmp_path / "h2o.mol"  # type: ignore[operator]
        mol2 = tmp_path / "nh3.mol"  # type: ignore[operator]
        dal.write_text("**DALTON")
        mol1.write_text("mol1")
        mol2.write_text("mol2")
        jobs, array_mode = mod.build_jobs([str(dal), str(mol1), str(mol2)], config)
        assert len(jobs) == 2
        assert array_mode is True

    def test_build_jobs_with_pot(self, tmp_path: object) -> None:
        """Pot file included in job."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        dal = tmp_path / "sp.dal"  # type: ignore[operator]
        mol = tmp_path / "h2o.mol"  # type: ignore[operator]
        pot = tmp_path / "water.pot"  # type: ignore[operator]
        dal.write_text("**DALTON")
        mol.write_text("mol")
        pot.write_text("pot data")
        jobs, _ = mod.build_jobs([str(dal), str(mol), str(pot)], config)
        assert len(jobs) == 1
        assert "water.pot" in jobs[0]

    def test_determine_job_name_single(self, tmp_path: object) -> None:
        """Single job name from dal_mol stem."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        dal = tmp_path / "sp.dal"  # type: ignore[operator]
        mol = tmp_path / "h2o.mol"  # type: ignore[operator]
        dal.write_text("**DALTON")
        mol.write_text("mol")
        mod.build_jobs([str(dal), str(mol)], config)
        config.array_mode = False
        name = mod.determine_job_name(config)
        assert name == "sp_h2o"

    def test_determine_job_name_array(self, tmp_path: object) -> None:
        """Array job name format."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        dal = tmp_path / "sp.dal"  # type: ignore[operator]
        mol1 = tmp_path / "h2o.mol"  # type: ignore[operator]
        mol2 = tmp_path / "nh3.mol"  # type: ignore[operator]
        dal.write_text("**DALTON")
        mol1.write_text("mol1")
        mol2.write_text("mol2")
        mod.build_jobs([str(dal), str(mol1), str(mol2)], config)
        config.array_mode = True
        config.throttle = 5
        name = mod.determine_job_name(config)
        assert name == "dalton-array-2t5"

    def test_create_exec_manifest(self, tmp_path: object) -> None:
        """Manifest file written."""
        import os

        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        dal = tmp_path / "sp.dal"  # type: ignore[operator]
        mol = tmp_path / "h2o.mol"  # type: ignore[operator]
        dal.write_text("**DALTON")
        mol.write_text("mol")
        mod.build_jobs([str(dal), str(mol)], config)
        old_cwd = os.getcwd()
        os.chdir(str(tmp_path))
        try:
            path = mod.create_exec_manifest("test")
            assert os.path.isfile(path)
        finally:
            os.chdir(old_cwd)

    def test_backup_targets(self) -> None:
        """Backup targets includes .out, .log."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        targets = mod.backup_targets("sp_h2o", "output/", config)
        assert "output/sp_h2o.out" in targets
        assert "output/sp_h2o.log" in targets

    def test_build_jobs_embedded_geometry(self, tmp_path: object) -> None:
        """DAL with embedded geometry (no .mol needed)."""
        mod = DaltonModule()
        config = init_runtime_config(Defaults())
        dal = tmp_path / "sp.dal"  # type: ignore[operator]
        dal.write_text("BASIS\ncc-pVDZ\ntest\n")
        jobs, _ = mod.build_jobs([str(dal)], config)
        assert len(jobs) == 1
