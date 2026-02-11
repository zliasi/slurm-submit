#!/usr/bin/env bats

setup() {
  load test-helpers
  source_libraries
  PROGRAM_INVOCATION="test"

  MOD_NAME="test"
  MOD_INPUT_EXTENSIONS=(".inp")
  MOD_MEMORY_UNIT="gb"
  MOD_DEFAULT_CPUS=1
  MOD_DEFAULT_MEMORY_GB=2

  apply_module_defaults
  init_runtime_globals
}

@test "parse_common_args sets cpus from -c" {
  parse_common_args -c 4 test.inp
  assert_equal "${NUM_CPUS}" "4"
}

@test "parse_common_args sets memory from -m" {
  parse_common_args -m 8 test.inp
  assert_equal "${MEMORY_GB}" "8"
}

@test "parse_common_args sets partition from -p" {
  parse_common_args -p kemi test.inp
  assert_equal "${PARTITION}" "kemi"
}

@test "parse_common_args sets time from -t" {
  parse_common_args -t 1-12:00:00 test.inp
  assert_equal "${TIME_LIMIT}" "1-12:00:00"
}

@test "parse_common_args sets output dir from -o" {
  parse_common_args -o mydir test.inp
  assert_equal "${OUTPUT_DIR}" "mydir"
}

@test "parse_common_args sets manifest from -M" {
  parse_common_args -M manifest.txt
  assert_equal "${MANIFEST_FILE}" "manifest.txt"
}

@test "parse_common_args sets throttle from -T" {
  parse_common_args -T 20 test.inp
  assert_equal "${THROTTLE}" "20"
}

@test "parse_common_args sets job name from -j" {
  parse_common_args -j myjob test.inp
  assert_equal "${CUSTOM_JOB_NAME}" "myjob"
}

@test "parse_common_args collects positional args" {
  parse_common_args file1.inp file2.inp -c 2
  assert_equal "${#POSITIONAL_ARGS[@]}" "2"
  assert_equal "${POSITIONAL_ARGS[0]}" "file1.inp"
  assert_equal "${POSITIONAL_ARGS[1]}" "file2.inp"
}

@test "parse_common_args collects unknown flags as remaining" {
  parse_common_args --unknown-flag -c 2 test.inp
  assert_equal "${#REMAINING_ARGS[@]}" "1"
  assert_equal "${REMAINING_ARGS[0]}" "--unknown-flag"
}

@test "parse_common_args handles --no-archive" {
  parse_common_args --no-archive test.inp
  assert_equal "${CREATE_ARCHIVE}" "false"
}

@test "validate_common_args accepts valid integer memory" {
  NUM_CPUS=1
  MEMORY_GB=4
  run validate_common_args
  assert_success
}

@test "validate_common_args accepts float memory for gb_float modules" {
  MOD_MEMORY_UNIT="gb_float"
  NUM_CPUS=1
  MEMORY_GB="0.5"
  run validate_common_args
  assert_success
}
