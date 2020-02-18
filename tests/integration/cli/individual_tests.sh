table2table() {
  mover table2table "${source_schema_name:?}" "${source_table_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

table2gsheet() {
  mover table2gsheet "${source_schema_name:?}" "${source_table_name:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

table2recordsdir() {
  mover table2recordsdir --target.variant=bluelabs "${source_schema_name:?}" "${source_table_name:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

gsheet2table() {
  mover gsheet2table "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

gsheet2gsheet() {
  mover gsheet2gsheet "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

gsheet2recordsdir() {
  mover gsheet2recordsdir "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

recordsdir2table() {
  mover recordsdir2table "${source_recordsdir_url:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}


recordsdir2gsheet() {
  mover recordsdir2gsheet "${source_recordsdir_url:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

recordsdir2recordsdir() {
  mover recordsdir2recordsdir "${source_recordsdir_url:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

url2table() {
  mover url2table "${source_csv_url:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

url2gsheet() {
  mover url2gsheet "${source_csv_url:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

url2recordsdir() {
  mover url2recordsdir "${source_csv_url:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

csv2table() {
  mover csv2table "${source_csv_path:?}" "${target_schema_name:?}" "${target_table_name:?}"
  assert_target_table_is_valid
}

csv2gsheet() {
  mover csv2gsheet "${source_csv_path:?}" "${target_spreadsheet_id:?}" "${target_sheet_name:?}" "${gcp_creds_name:?}"
  assert_target_gsheet_is_valid
}

csv2recordsdir() {
  mover csv2recordsdir "${source_csv_path:?}" "${target_recordsdir_url:?}"
  assert_target_recordsdir_is_valid
}

csv2url() {
  mover csv2url "${source_csv_path:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

csv2csv() {
  mover csv2csv "${source_csv_path:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

table2url() {
  mover table2url "${source_schema_name:?}" "${source_table_name:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}


table2csv() {
  mover table2csv "${source_schema_name:?}" "${source_table_name:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

gsheet2url() {
  mover gsheet2url "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

gsheet2csv() {
  mover gsheet2csv "${source_spreadsheet_id:?}" "${source_sheet_name:?}" "${gcp_creds_name:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

recordsdir2url() {
  mover recordsdir2url "${source_recordsdir_url:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

recordsdir2csv() {
  mover recordsdir2csv "${source_recordsdir_url:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}

url2url() {
  mover url2url "${source_csv_url:?}" "${target_csv_url:?}"
  assert_target_csv_url_is_valid
}

url2csv() {
  mover url2csv "${source_csv_url:?}" "${target_csv_path:?}"
  assert_target_csv_is_valid
}
