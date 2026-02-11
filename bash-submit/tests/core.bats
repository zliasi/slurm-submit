#!/usr/bin/env bats

setup() {
  load test-helpers
  source "${LIB_DIR}/core.sh"
  PROGRAM_INVOCATION="test"
}

@test "die exits with code 1 and prints message" {
  run die "test error"
  assert_failure
  assert_output --partial "test error"
}

@test "die_usage exits with code 1 and shows usage hint" {
  run die_usage "bad arg"
  assert_failure
  assert_output --partial "bad arg"
  assert_output --partial "for help"
}

@test "validate_file_exists passes for existing file" {
  local tmpfile
  tmpfile=$(mktemp)
  run validate_file_exists "${tmpfile}"
  assert_success
  rm -f "${tmpfile}"
}

@test "validate_file_exists fails for missing file" {
  run validate_file_exists "/nonexistent/path"
  assert_failure
  assert_output --partial "File not found"
}

@test "validate_positive_integer accepts valid integers" {
  run validate_positive_integer "1" "test"
  assert_success
  run validate_positive_integer "42" "test"
  assert_success
  run validate_positive_integer "100" "test"
  assert_success
}

@test "validate_positive_integer rejects invalid values" {
  run validate_positive_integer "0" "test"
  assert_failure
  run validate_positive_integer "-1" "test"
  assert_failure
  run validate_positive_integer "abc" "test"
  assert_failure
  run validate_positive_integer "1.5" "test"
  assert_failure
}

@test "validate_positive_number accepts integers and floats" {
  run validate_positive_number "1" "test"
  assert_success
  run validate_positive_number "0.5" "test"
  assert_success
  run validate_positive_number "3.14" "test"
  assert_success
}

@test "validate_positive_number rejects invalid values" {
  run validate_positive_number "0" "test"
  assert_failure
  run validate_positive_number "-1" "test"
  assert_failure
  run validate_positive_number "abc" "test"
  assert_failure
}

@test "validate_time_format accepts valid formats" {
  run validate_time_format "01:00:00"
  assert_success
  run validate_time_format "1-12:30:00"
  assert_success
  run validate_time_format ""
  assert_success
}

@test "validate_time_format rejects invalid formats" {
  run validate_time_format "abc"
  assert_failure
  run validate_time_format "25:00:00"
  assert_failure
}

@test "validate_file_extension accepts matching extensions" {
  run validate_file_extension "test.inp" ".inp" ".xyz"
  assert_success
  run validate_file_extension "test.xyz" ".inp" ".xyz"
  assert_success
}

@test "validate_file_extension rejects non-matching extensions" {
  run validate_file_extension "test.txt" ".inp" ".xyz"
  assert_failure
  assert_output --partial "Invalid extension"
}

@test "validate_file_extension accepts anything when no extensions given" {
  run validate_file_extension "test.anything"
  assert_success
}

@test "strip_extension removes extension from basename" {
  run strip_extension "path/to/molecule.inp" ".inp"
  assert_output "molecule"
}

@test "normalize_output_dir adds trailing slash" {
  run normalize_output_dir "output"
  assert_output "output/"
  run normalize_output_dir "output/"
  assert_output "output/"
}

@test "to_absolute_path converts relative to absolute" {
  local result
  result=$(to_absolute_path "relative/path")
  [[ "${result}" = /* ]]
}
