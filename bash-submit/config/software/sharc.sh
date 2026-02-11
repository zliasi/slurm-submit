#!/usr/bin/env bash
SHARC_HOME="${HOME}/software/build/sharc/default"

SHARC_DEPS="source /software/kemi/intel/oneapi/setvars.sh --force
export SHARC=${SHARC_HOME}/bin
export PATH=\$SHARC:\$PATH"
