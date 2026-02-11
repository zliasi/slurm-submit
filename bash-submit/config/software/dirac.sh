#!/usr/bin/env bash
DIRAC_PAM="/lustre/hpc/software/kemi/DIRAC-21.0/build-intelmpi-2020.4.304-i8/pam"

DIRAC_DEPS='export MKL_BLACS_MPI=INTELMPI
module load intel/20.0.4
module load intelmpi/2020.4.304'
