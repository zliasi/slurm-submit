"""Module registry with eager imports."""

from __future__ import annotations

from typing import Callable

from slurm_submit.module_base import SubmitModule

MODULE_REGISTRY: dict[str, type[SubmitModule]] = {}


def register_module(name: str) -> Callable[[type[SubmitModule]], type[SubmitModule]]:
    """Decorator to register a module class.

    Args:
        name: Module name (e.g. "orca").

    Returns:
        Class decorator.
    """

    def _decorator(cls: type[SubmitModule]) -> type[SubmitModule]:
        """Register cls in MODULE_REGISTRY under name.

        Args:
            cls: Module class to register.

        Returns:
            The unmodified class.
        """
        MODULE_REGISTRY[name] = cls
        return cls

    return _decorator


def _load_modules() -> None:
    """Eagerly import all module files to trigger registration."""
    from slurm_submit.modules import (  # noqa: F401
        cfour,
        dalton,
        dirac,
        exec_mod,
        gaussian,
        molpro,
        nwchem,
        orca,
        python_mod,
        sharc,
        std2,
        turbomole,
        xtb,
    )


_load_modules()
