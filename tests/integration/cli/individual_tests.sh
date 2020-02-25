table2table() {
  mvrec table2table "${source_db_name:?}" "${source_schema_name:?}" "${source_table_name:?}" "${target_db_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

table2gsheet() {
  mvrec table2gsheet "${source_db_name:?}" "${source_schema_name:?}" "${source_table_name:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

table2recordsdir() {
  mvrec table2recordsdir --target.variant=bluelabs "${source_db_name:?}" "${source_schema_name:?}" "${source_table_name:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

gsheet2table() {
  mvrec gsheet2table "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_db_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

gsheet2gsheet() {
  mvrec gsheet2gsheet "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

gsheet2recordsdir() {
  mvrec gsheet2recordsdir "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

recordsdir2table() {
  mvrec recordsdir2table "${source_recordsdir_url:?}" "${target_db_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}


recordsdir2gsheet() {
  mvrec recordsdir2gsheet "${source_recordsdir_url:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

recordsdir2recordsdir() {
  mvrec recordsdir2recordsdir "${source_recordsdir_url:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

url2table() {
  mvrec url2table "${source_csv_url:?}" "${target_db_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

url2gsheet() {
  mvrec url2gsheet "${source_csv_url:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

url2recordsdir() {
  mvrec url2recordsdir "${source_csv_url:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

file2table() {
  mvrec file2table "${source_csv_path:?}" "${target_db_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

file2gsheet() {
  mvrec file2gsheet "${source_csv_path:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

file2recordsdir() {
  mvrec file2recordsdir "${source_csv_path:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

file2url() {
  mvrec file2url "${source_csv_path:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

file2file() {
  mvrec file2file "${source_csv_path:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

table2url() {
  mvrec table2url "${source_db_name:?}" "${source_schema_name:?}" "${source_table_name:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}


table2file() {
  mvrec table2file "${source_db_name:?}" "${source_schema_name:?}" "${source_table_name:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

gsheet2url() {
  mvrec gsheet2url "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

gsheet2file() {
  mvrec gsheet2file "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

recordsdir2url() {
  mvrec recordsdir2url "${source_recordsdir_url:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

recordsdir2file() {
  mvrec recordsdir2file "${source_recordsdir_url:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

url2url() {
  mvrec url2url "${source_csv_url:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

url2file() {
  mvrec url2file "${source_csv_url:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}
