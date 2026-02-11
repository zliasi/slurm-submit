"""SLURM job submission toolkit."""

import logging

logger = logging.getLogger("slurm_submit")

_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)
