#!/usr/bin/env bats

setup() {
  load test-helpers
  setup_tmpdir
  create_mock_sbatch "${TEST_TMPDIR}"
  create_test_inputs
}

teardown() {
  teardown_tmpdir
}

@test "submit resolves module from s<name> symlink" {
  ln -sf submit "${TEST_TMPDIR}/sorca"
  run "${TEST_TMPDIR}/sorca" --help
  assert_output --partial "ORCA"
}

@test "submit resolves module from first argument" {
  run "${BIN_DIR}/submit" orca --help
  assert_output --partial "ORCA"
}

@test "submit rejects unknown module" {
  run "${BIN_DIR}/submit" nonexistent
  assert_failure
  assert_output --partial "Unknown module"
}

@test "submit shows help with -h" {
  run "${BIN_DIR}/submit" orca -h
  assert_success
  assert_output --partial "ORCA"
}

@test "submit orca generates valid sbatch script" {
  "${BIN_DIR}/submit" orca "${INPUTS_DIR}/test.inp" \
    -c 2 -m 4 -o "${TEST_TMPDIR}/output"

  assert [ -f "${MOCK_SBATCH_CAPTURE}" ]
  run bash -n "${MOCK_SBATCH_CAPTURE}"
  assert_success
}

@test "submit xtb generates valid sbatch script" {
  "${BIN_DIR}/submit" xtb "${INPUTS_DIR}/test.xyz" \
    -c 1 -m 1 -o "${TEST_TMPDIR}/output"

  assert [ -f "${MOCK_SBATCH_CAPTURE}" ]
  run bash -n "${MOCK_SBATCH_CAPTURE}"
  assert_success
}

@test "submit python generates valid sbatch script" {
  "${BIN_DIR}/submit" python "${INPUTS_DIR}/test.py" \
    -c 1 -m 1 -o "${TEST_TMPDIR}/output"

  assert [ -f "${MOCK_SBATCH_CAPTURE}" ]
  run bash -n "${MOCK_SBATCH_CAPTURE}"
  assert_success
}
