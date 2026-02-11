#!/usr/bin/env bats

setup() {
  load test-helpers
  source "${LIB_DIR}/core.sh"
  source "${PROJECT_ROOT}/config/defaults.sh"
  source "${LIB_DIR}/backup.sh"
  setup_tmpdir
}

teardown() {
  teardown_tmpdir
}

@test "backup_existing_file does nothing for missing file" {
  run backup_existing_file "${TEST_TMPDIR}/nonexistent"
  assert_success
}

@test "backup_existing_file rotates to .0" {
  local target="${TEST_TMPDIR}/test.out"
  printf "content" >"${target}"

  backup_existing_file "${target}"
  assert [ ! -f "${target}" ]
  assert [ -f "${TEST_TMPDIR}/backup/test.out.0" ]
}

@test "backup_existing_file creates backup directory" {
  local target="${TEST_TMPDIR}/test.out"
  printf "content" >"${target}"

  backup_existing_file "${target}"
  assert [ -d "${TEST_TMPDIR}/backup" ]
}

@test "backup_existing_file rotates multiple backups" {
  local target="${TEST_TMPDIR}/test.out"

  printf "first" >"${target}"
  backup_existing_file "${target}"

  printf "second" >"${target}"
  backup_existing_file "${target}"

  assert [ -f "${TEST_TMPDIR}/backup/test.out.0" ]
  assert [ -f "${TEST_TMPDIR}/backup/test.out.1" ]

  run cat "${TEST_TMPDIR}/backup/test.out.0"
  assert_output "second"
  run cat "${TEST_TMPDIR}/backup/test.out.1"
  assert_output "first"
}

@test "emit_backup_function_inline produces valid bash" {
  local output
  output=$(emit_backup_function_inline)
  run bash -n <(printf "%s" "${output}")
  assert_success
}
