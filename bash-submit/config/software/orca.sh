#!/usr/bin/env bash
ORCA_PATH="/groups/kemi/liasi/local/orca/latest"
OPENMPI_BIN="${ORCA_PATH}/openmpi/bin"
OPENMPI_LIB="${ORCA_PATH}/openmpi/lib"

ORCA_DEPS="module purge
export PATH=${ORCA_PATH}:${OPENMPI_BIN}:\$PATH
export LD_LIBRARY_PATH=${OPENMPI_LIB}:\$LD_LIBRARY_PATH
export OMP_NUM_THREADS=1
export OMPI_MCA_rmaps_base_oversubscribe=1"
