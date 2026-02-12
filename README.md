# slurm-submit

A unified Slurm job submission toolkit for computational chemistry software.

`slurm-submit` wraps the manual writing of multiple software-specific submit scripts into a
set of simple, per-software commands. It handles scratch directories, file backup with
rotation, automatic job arrays, archive creation, among other things.

The project includes two independent implementations with identical functionality:

- `bash-submit`: Pure Bash, sourceable with no dependencies beyond Bash 4+.
- `python-submit`: Python package, installable via `uv`/`pip`.

Both share the same configuration format and module set. Pick whichever fits
your preferences and environment the best.

## Installation

Clone or download the repository:

```
$ git clone https://github.com/zliasi/slurm-submit.git
$ cd slurm-submit
```

### Bash version

Adds `bash-submit/bin/` to `$PATH` for the current shell session:

```
$ source bash-submit/setup.sh
```

### Python version

Installs entry points (`sorca`, `sdalton`, etc.) into the active Python
environment's `bin/` directory:

```
$ cd python-submit
$ uv pip install -e .
```

## Usage

```
$ sorca input.inp -c 2 -m 8
$ sgaussian input.com -m 4 -t 2-00:00:00
$ sdalton input.dal input.mol
```

Common flags:

```
-c, --cpus INT             CPU cores per task (default: 1)
-m, --memory NUM           Total memory in GB (default: 2)
-p, --partition NAME       Partition (default: chem)
-t, --time D-HH:MM:SS      Time limit (default: partition max)
-o, --output DIR           Output directory (default: ./output)
-M, --manifest FILE        Manifest file (job array)
-T, --throttle INT         Max concurrent array subjobs (default: 5)
-N, --nodes INT            Number of nodes (default: 1)
-n, --ntasks INT           Number of tasks (default: 1)
-j, --job-name NAME        Custom job name
--variant NAME             Software variant
--export [FILE]            Export sbatch script to file
--no-archive               Disable archive creation (default: enabled)
-h, --help                 Show help
```

## Variants

Load alternate software configurations with `--variant`:

```
$ sdalton input.dal input.mol --variant dev
```

This loads `dalton-dev.sh` (or `dalton-dev.toml`) instead of `dalton.sh`.
Variant files use the naming convention `<module>-<variant>`.
Missing variant files produce an error (no silent fallback).

Place variant configs alongside base configs:

```
config/software/dalton.toml
config/software/dalton-dev.toml
config/software/dalton-stable.toml
```

## Export

Write the generated sbatch script to a file instead of submitting:

```
$ sorca test.inp -c 4 --export job.slurm
$ sorca test.inp -c 4 --export
```

With no filename provided, defaults to `<module>.slurm` (e.g., `orca.slurm`). Useful for quick testing.

## Configuration

Configuration is layered (low to high priority):

1. `config/defaults.sh` (or `defaults.toml`); shipped defaults
2. Module metadata defaults
3. `config/software/<module>.sh` (or `.toml`); site-specific paths and deps
4. CLI arguments

## Modules

| Module | Command | Software | Notes |
|--------|---------|----------|-------|
| orca | `sorca` | ORCA | scratch, archive |
| gaussian | `sgaussian` | Gaussian | scratch, .com/.gjf |
| dalton | `sdalton` | Dalton | multi-file (dal/mol/pot), 32i/64i |
| dirac | `sdirac` | DIRAC | multi-file (inp/mol) |
| turbomole | `sturbomole` | Turbomole | directory-based |
| cfour | `scfour` | CFOUR | scratch, archive |
| molpro | `smolpro` | Molpro | scratch |
| nwchem | `snwchem` | NWChem | scratch |
| sharc | `ssharc` | SHARC | dynamics |
| xtb | `sxtb` | xTB | float memory, passthrough args |
| std2 | `sstd2` | STD2 | dual mode (molden/xtb) |
| python | `spython` | Python | conda/venv/uv, float memory |
| exec | `sexec` | Generic | arbitrary command, --mpi |

Invocation forms:

```
$ submit <module> [input...] [options]
$ <command> [input...] [options]
```

For example, `submit orca input.inp -c 4` and `sorca input.inp -c 4` are equivalent.
