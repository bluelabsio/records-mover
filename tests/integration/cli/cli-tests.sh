#!/bin/bash -eux

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

records_schema_json_schema="${DIR}/../records/records_schema_v1_schema.json"

current_epoch=$(date +%s)

source_schema_name=${DB_USERNAME}
source_table_name=test_cli_source_table
source_schema_and_table=${source_schema_name}.${source_table_name}

target_schema_name=${DB_USERNAME}
target_table_name_prefix=test_cli_target_table

# add CIRCLE_BUILD_NUM as a salt for uniqueness if it's available
target_table_name=${target_table_name_prefix}_${CIRCLE_BUILD_NUM:-local}_${current_epoch}
target_schema_and_table=${target_schema_name}.${target_table_name}

target_sheet_name="target_${CIRCLE_BUILD_NUM:-local}_${current_epoch}"
target_spreadsheet_id="15Q9yNnrMg5_b8bXVRz01cV7MZ0rm7AkyNDXc3WEX2a0"
# To make one of these, follow the instructions here and upload
# (e.g. to a LastPass note) the credentials.json file:
# https://developers.google.com/sheets/api/quickstart/python
gcp_creds_name="bq_itest Google Service Account"

source_csv_path="$(pwd)/data.csv"
source_csv_url="file://${source_csv_path}"

source_recordsdir_path="$(pwd)/source_rdir/"
source_recordsdir_url="file://${source_recordsdir_path}"

target_recordsdir_path="$(pwd)/target_rdir/"
target_recordsdir_url="file://${target_recordsdir_path}"

target_csv_path="$(pwd)/target.csv"
target_csv_url="file://${target_csv_path}"

source_sheet_name="source"
source_spreadsheet_id="15Q9yNnrMg5_b8bXVRz01cV7MZ0rm7AkyNDXc3WEX2a0"

clear_target_csv() {
  rm -fr "${target_csv_path}" || true
}

clear_tables() {
  db-connect <<< "DROP TABLE IF EXISTS ${target_schema_and_table};"
}

clear_sheets() {
  mvrec csv2gsheet empty.csv "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"

}

clear_rdir() {
  rm -fr "${target_recordsdir_path}" || true
}

one_time_setup() {
  db-connect <<< "CREATE SCHEMA IF NOT EXISTS ${source_schema_name};" || true
  db-connect <<< "CREATE SCHEMA IF NOT EXISTS ${target_schema_name};" || true
  # generate manifest of source_rdir with correct paths
  data_file="${source_recordsdir_path}/data.csv.gz"
  echo '{"entries": [{"url": "file://'"${data_file}"'", "mandatory": true}]}' > "${source_recordsdir_path}/_manifest"
  "${DIR:?}/../records/purge_old_test_sheets.py" "${gcp_creds_name}" "${target_spreadsheet_id}"
  "${DIR:?}/../records/purge_old_test_tables.py" "${target_schema_name}" "${target_table_name_prefix}"
}

setup() {
  clear_tables
  clear_sheets
  clear_rdir
  clear_target_csv
  mkdir "${target_recordsdir_path}"
  # You can't do 'create table if not exists as select' in Redshift 🤦
  #
  # https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_AS.html
  db-connect <<< "CREATE TABLE  ${source_schema_and_table} AS SELECT 1 as a;" || true
  echo "done with setup"
}

teardown() {
  clear_tables
  clear_sheets
  clear_rdir
}

assert_target_recordsdir_is_valid() {
  # we try to match source format when not specifying a target format,
  # so compression and header rows can vary here in tests:
  expected_filename=expected_target_recordsdir.csv
  if [ "$(jq .hints\[\"header-row\"\] "${target_recordsdir_path}"/_format_delimited)" == true ]
  then
    expected_filename=expected_target_recordsdir_with_header.csv
  fi
  if [ "$(jq .hints.compression "${target_recordsdir_path}"/_format_delimited)" == null ]
  then
    diff -u "${expected_filename}" <(cat "${target_recordsdir_path}"/*.csv)
  else
    diff -u "${expected_filename}" <(gzcat "${target_recordsdir_path}"/*.gz)
  fi

  jsonschema -i "${target_recordsdir_path}"/_schema.json "${records_schema_json_schema}"
}


assert_target_csv_is_valid() {
  diff -u expected_target_recordsdir.csv "${target_csv_path}"
}

assert_target_csv_url_is_valid() {
  assert_target_csv_is_valid
}


assert_target_table_is_valid() {
  mvrec table2recordsdir --target.variant=bluelabs "${target_schema_name}" "${target_table_name}" "${target_recordsdir_url}"
  assert_target_recordsdir_is_valid
}

assert_target_gsheet_is_valid() {
  mvrec gsheet2recordsdir --source.out_of_band_column_headers="a" "${target_spreadsheet_id}" "${target_sheet_name}" "${gcp_creds_name}" "${target_recordsdir_url}"
  assert_target_recordsdir_is_valid
}

. individual_tests.sh

one_time_setup

test_type=${1-all}

first_third_sources="table"
second_third_sources="gsheet csv"
third_third_sources="recordsdir url"

if [ "${test_type}" == ci_1 ]
then
  sources="${first_third_sources}"
elif [ "${test_type}" == ci_2 ]
then
  sources="${second_third_sources}"
elif [ "${test_type}" == ci_3 ]
then
  sources="${third_third_sources}"
elif [ "${test_type}" == all ]
then
  sources="${first_third_sources} ${second_third_sources} ${third_third_sources}"
else
  >&2 echo "Valid options: all, ci_1, ci_2"
  exit 1
fi

for source in ${sources}
do
  for target in csv table gsheet recordsdir url
  do
    setup
    eval "${source}2${target}"
    teardown
  done
done
