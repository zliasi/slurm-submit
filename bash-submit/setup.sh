#!/usr/bin/env bash
set -euo pipefail

# Setup script for bash-submit
#
# Creates symlinks for each module (sorca, sxtb, ...),
# initializes bats test submodules, and offers to make
# commands callable via PATH or ~/bin/ symlinks.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly SCRIPT_DIR
readonly BIN_DIR="${SCRIPT_DIR}/bin"
readonly MODULES_DIR="${SCRIPT_DIR}/modules"
readonly TESTS_LIB_DIR="${SCRIPT_DIR}/tests/lib"
readonly HOME_BIN_DIR="${HOME}/bin"
readonly BASHRC_PATH="${HOME}/.bashrc"
readonly MAX_PROMPT_ATTEMPTS=5

# Creates module symlinks in bin/
#
# Exit codes:
#   0 - Success
create_symlinks() {
  local module_file module_name link_path

  for module_file in "${MODULES_DIR}"/*.sh; do
    [[ -f "${module_file}" ]] || continue
    module_name=$(basename "${module_file}" .sh)
    link_path="${BIN_DIR}/s${module_name}"

    if [[ -L "${link_path}" ]]; then
      printf "Symlink exists: %s\n" "${link_path}"
    else
      ln -s submit "${link_path}"
      printf "Created symlink: %s -> submit\n" "${link_path}"
    fi
  done
}

# Initializes bats test submodules
#
# Exit codes:
#   0 - Success
init_bats() {
  if [[ -d "${SCRIPT_DIR}/.git" ]] \
    || git -C "${SCRIPT_DIR}" rev-parse --git-dir >/dev/null 2>&1; then
    printf "Initializing bats submodules...\n"
    git -C "${SCRIPT_DIR}" submodule update --init "${TESTS_LIB_DIR}" \
      2>/dev/null || true
  fi

  if [[ -f "${TESTS_LIB_DIR}/bats-core/bin/bats" ]]; then
    printf "Bats available at: %s/bats-core/bin/bats\n" "${TESTS_LIB_DIR}"
  else
    printf "Note: bats not found. Install manually or add as git submodules:\n"
    printf "  git submodule add https://github.com/bats-core/bats-core tests/lib/bats-core\n"
    printf "  git submodule add https://github.com/bats-core/bats-support tests/lib/bats-support\n"
    printf "  git submodule add https://github.com/bats-core/bats-assert tests/lib/bats-assert\n"
  fi
}

# Prompts user to choose how commands are made available
#
# Prints menu to stderr so stdout captures only the choice.
#
# Returns:
#   Prints choice (1 or 2) to stdout
#
# Exit codes:
#   0 - Valid choice made
#   1 - Max attempts exceeded
prompt_install_method() {
  local choice attempt

  printf "\nHow should the commands be made available?\n" >&2
  printf "  1) Add to PATH in ~/.bashrc (default)\n" >&2
  printf "  2) Create symlinks in ~/bin/\n" >&2

  for ((attempt = 1; attempt <= MAX_PROMPT_ATTEMPTS; attempt++)); do
    printf "Choice [1]: " >&2
    read -r choice
    choice="${choice:-1}"

    if [[ "${choice}" =~ ^[12]$ ]]; then
      printf "%s" "${choice}"
      return 0
    fi

    printf "Invalid choice. Please enter 1 or 2.\n" >&2
  done

  printf "Error: Max attempts exceeded\n" >&2
  return 1
}

# Appends bin directory to PATH in ~/.bashrc if not already present
#
# Exit codes:
#   0 - Success (line added or already present)
install_bashrc_path() {
  if grep -qF "export PATH=\"${BIN_DIR}:" "${BASHRC_PATH}" 2>/dev/null
  then
    printf "PATH already configured in %s\n" "${BASHRC_PATH}"
    return 0
  fi

  printf '\nexport PATH="%s:$PATH"\n' "${BIN_DIR}" >> "${BASHRC_PATH}"
  printf "Added %s to PATH in %s\n" "${BIN_DIR}" "${BASHRC_PATH}"
}

# Creates symlinks in ~/bin/ for submit and all module commands
#
# Creates ~/bin/ if it does not exist. Skips symlinks that already
# point to the correct target. Warns if a symlink points elsewhere.
#
# Exit codes:
#   0 - Success
install_bin_symlinks() {
  local module_file module_name link_name link_path
  local target existing_target resolved_target
  local -a link_names

  mkdir -p "${HOME_BIN_DIR}"

  target="${BIN_DIR}/submit"
  link_names=("submit")

  for module_file in "${MODULES_DIR}"/*.sh; do
    [[ -f "${module_file}" ]] || continue
    module_name=$(basename "${module_file}" .sh)
    link_names+=("s${module_name}")
  done

  for link_name in "${link_names[@]}"; do
    link_path="${HOME_BIN_DIR}/${link_name}"

    if [[ -L "${link_path}" ]]; then
      existing_target=$(readlink -f "${link_path}")
      resolved_target=$(readlink -f "${target}")

      if [[ "${existing_target}" == "${resolved_target}" ]]; then
        printf "Symlink exists: %s\n" "${link_path}"
        continue
      fi

      printf "Warning: %s points to %s, not %s\n" \
        "${link_path}" "${existing_target}" "${resolved_target}" >&2
      continue
    fi

    if [[ -e "${link_path}" ]]; then
      printf "Warning: %s exists and is not a symlink\n" \
        "${link_path}" >&2
      continue
    fi

    ln -s "${target}" "${link_path}"
    printf "Created symlink: %s -> %s\n" "${link_path}" "${target}"
  done
}

# Dispatches install based on user choice
#
# Arguments:
#   $1 - choice: Install method (1=PATH, 2=symlinks)
#
# Exit codes:
#   0 - Success
#   1 - Invalid choice
run_install() {
  local choice="$1"

  [[ -n "${choice}" ]] || {
    printf "Error: Missing choice parameter\n" >&2
    return 1
  }

  case "${choice}" in
    1)
      install_bashrc_path
      ;;
    2)
      install_bin_symlinks
      ;;
    *)
      printf "Error: Invalid choice '%s'\n" "${choice}" >&2
      return 1
      ;;
  esac
}

# Runs full setup: internal symlinks, bats init, install prompt
#
# Exit codes:
#   0 - Success
main() {
  printf "Setting up bash-submit...\n\n"

  create_symlinks
  printf "\n"
  init_bats

  local install_choice
  install_choice=$(prompt_install_method)
  printf "\n"
  run_install "${install_choice}"

  printf "\nSetup complete.\n"
}

main "$@"
