version 1.0

import "../tasks/task_csv_prep.wdl" as csv

workflow redcap_prep {
  input {    
    String workspace_name
    String project_name
    String table_name
    Array[String] sample_names
    String record_id_column_name
    String wgs_id_column_name 
  }

  call csv.datetime {}

  call csv.prep_csv {
    input:
      workspace_name = workspace_name,
      project_name = project_name,
      table_name = table_name,
      sample_names = sample_names,
      record_id_column_name = record_id_column_name,
      wgs_id_column_name = wgs_id_column_name,
      timestamp = datetime.timestamp
  }

  output {
    File redcap_input = prep_csv.redcap_input
    String readcap_prep_version = "v0.1"
  }

}