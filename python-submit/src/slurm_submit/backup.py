"""Rotating file backup -- pre-submit and inline sbatch embedding."""

from __future__ import annotations

import fcntl
import io
import logging
import os

logger = logging.getLogger("slurm_submit")


def _rotate_backups(backup_base: str, max_backups: int) -> None:
    """Rotate numbered backups: .N-1 -> .N, ..., .0 -> .1.

    Args:
        backup_base: Base path for backup files (without number suffix).
        max_backups: Maximum number of backup copies.
    """
    assert max_backups > 0, "max_backups must be positive"
    width = len(str(max_backups))

    last = f"{backup_base}.{max_backups:0{width}d}"
    if os.path.exists(last):
        os.remove(last)

    for idx in range(max_backups - 1, -1, -1):
        src = f"{backup_base}.{idx:0{width}d}"
        dst = f"{backup_base}.{idx + 1:0{width}d}"
        if os.path.exists(src):
            os.rename(src, dst)


def backup_existing_file(
    target_path: str,
    use_backup_dir: bool,
    backup_dir_name: str,
    max_backups: int,
) -> None:
    """Backup an existing file with numbered rotation.

    Rotates target -> .0, .0 -> .1, ..., .N deleted.
    Uses flock for concurrent safety.

    Args:
        target_path: File to backup.
        use_backup_dir: Whether to store backups in a subdirectory.
        backup_dir_name: Name of backup subdirectory.
        max_backups: Maximum number of backup copies.
    """
    if not target_path or not os.path.isfile(target_path):
        return

    assert isinstance(backup_dir_name, str), "backup_dir_name must be str"
    assert max_backups > 0, "max_backups must be positive"

    dir_path = os.path.dirname(target_path) or "."
    base_name = os.path.basename(target_path)

    if use_backup_dir:
        backup_dir = os.path.join(dir_path, backup_dir_name)
        os.makedirs(backup_dir, exist_ok=True)
        backup_base = os.path.join(backup_dir, base_name)
    else:
        backup_base = os.path.join(dir_path, base_name)

    width = len(str(max_backups))
    lock_path = f"{backup_base}.lock"

    try:
        lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            _rotate_backups(backup_base, max_backups)
            os.rename(target_path, f"{backup_base}.{0:0{width}d}")
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)
            if os.path.exists(lock_path):
                os.remove(lock_path)
    except OSError:
        logger.warning("Could not acquire lock for backup of %s", target_path)


def _emit_backup_dir_block(
    out: io.StringIO, use_backup_dir: bool, backup_dir_name: str
) -> None:
    """Emit backup directory resolution block.

    Args:
        out: Output buffer.
        use_backup_dir: Whether to use subdirectory.
        backup_dir_name: Name of backup subdirectory.
    """
    if use_backup_dir:
        out.write(f'  if [[ "{use_backup_dir}" == true ]]; then\n')
        out.write(f'    backup_dir="${{dir_path}}/{backup_dir_name}"\n')
        out.write('    [[ ! -d "$backup_dir" ]] && mkdir -p "$backup_dir"\n')
        out.write('    backup_base="${backup_dir}/${base_name}"\n')
        out.write("  else\n")
        out.write('    backup_base="${dir_path}/${base_name}"\n')
        out.write("  fi\n")
    else:
        out.write('  backup_base="${dir_path}/${base_name}"\n')


def _emit_rotation_loop(out: io.StringIO, max_backups: int, width: int) -> None:
    """Emit the rotation loop and final move in bash.

    Args:
        out: Output buffer.
        max_backups: Maximum backup copies.
        width: Zero-padding width.
    """
    out.write(f'  printf -v to "%0${{width}}d" "{max_backups}"\n')
    out.write('  [[ -e "${backup_base}.${to}" ]] && rm -f -- "${backup_base}.${to}"\n')
    out.write("\n")
    out.write(f"  for ((i = {max_backups} - 1; i >= 0; i--)); do\n")
    out.write('    printf -v from "%0${width}d" "$i"\n')
    out.write('    printf -v to "%0${width}d" "$((i + 1))"\n')
    out.write('    if [[ -e "${backup_base}.${from}" ]]; then\n')
    out.write('      mv -f -- "${backup_base}.${from}" "${backup_base}.${to}"\n')
    out.write("    fi\n")
    out.write("  done\n")
    out.write("\n")
    out.write('  printf -v to "%0${width}d" 0\n')
    out.write('  mv -f -- "$target_path" "${backup_base}.${to}"\n')


def emit_backup_function_inline(
    out: io.StringIO,
    use_backup_dir: bool,
    backup_dir_name: str,
    max_backups: int,
) -> None:
    """Emit bash backup function for embedding inside sbatch scripts.

    Args:
        out: Output buffer.
        use_backup_dir: Whether to store backups in subdirectory.
        backup_dir_name: Name of backup subdirectory.
        max_backups: Maximum backup copies.
    """
    assert max_backups > 0, "max_backups must be positive"
    width = len(str(max_backups))

    out.write("backup_existing_files() {\n")
    out.write('  local target_path="$1"\n')
    out.write('  [[ -z "$target_path" || ! -f "$target_path" ]] && return 0\n')
    out.write("\n")
    out.write("  local dir_path base_name backup_dir backup_base\n")
    out.write('  dir_path=$(dirname -- "$target_path")\n')
    out.write('  base_name=$(basename -- "$target_path")\n')
    out.write("\n")

    _emit_backup_dir_block(out, use_backup_dir, backup_dir_name)

    out.write("\n")
    out.write(f"  local width={width}\n")
    out.write("  local from to i\n")
    out.write("\n")
    out.write('  exec 9>"${backup_base}.lock" || true\n')
    out.write("  flock -w 5 9 || true\n")
    out.write("\n")

    _emit_rotation_loop(out, max_backups, width)

    out.write("\n")
    out.write("  flock -u 9 || true\n")
    out.write('  rm -f "${backup_base}.lock" || true\n')
    out.write("}\n")
