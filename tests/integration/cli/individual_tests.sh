table2table() {
  mvrec table2table "${source_schema_name:?}" "${source_table_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

table2gsheet() {
  mvrec table2gsheet "${source_schema_name:?}" "${source_table_name:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

table2recordsdir() {
  mvrec table2recordsdir --target.variant=bluelabs "${source_schema_name:?}" "${source_table_name:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

gsheet2table() {
  mvrec gsheet2table "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
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
  mvrec recordsdir2table "${source_recordsdir_url:?}" "${target_schema_name:?}" "${target_table_name:?}"
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
  mvrec url2table "${source_csv_url:?}" "${target_schema_name:?}" "${target_table_name:?}"
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

csv2table() {
  mvrec csv2table "${source_csv_path:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

csv2gsheet() {
  mvrec csv2gsheet "${source_csv_path:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

csv2recordsdir() {
  mvrec csv2recordsdir "${source_csv_path:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

csv2url() {
  mvrec csv2url "${source_csv_path:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

csv2csv() {
  mvrec csv2csv "${source_csv_path:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

table2url() {
  mvrec table2url "${source_schema_name:?}" "${source_table_name:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}


table2csv() {
  mvrec table2csv "${source_schema_name:?}" "${source_table_name:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

gsheet2url() {
  mvrec gsheet2url "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

gsheet2csv() {
  mvrec gsheet2csv "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

recordsdir2url() {
  mvrec recordsdir2url "${source_recordsdir_url:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

recordsdir2csv() {
  mvrec recordsdir2csv "${source_recordsdir_url:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

url2url() {
  mvrec url2url "${source_csv_url:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

url2csv() {
  mvrec url2csv "${source_csv_url:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}
