version 1.0

import "../tasks/task_prep_tables.wdl" as prep

workflow SRA_prep {
  input {
    String project_name
    String workspace_name
    String table_name
    Array[String] sample_names
    String sra_transfer_gcp_bucket # used to be gcp_bucket_uri
    String bioproject
  }

  call prep.prep_tables {
    input:
      project_name = project_name,
      workspace_name = workspace_name,
      table_name = table_name,
      sample_names = sample_names,
      bioproject = bioproject,
      gcp_bucket_uri = sra_transfer_gcp_bucket
  }

  output {
    File biosample_metadata = prep_tables.biosample_table
    File sra_metadata = prep_tables.sra_table
    File excluded_samples = prep_tables.excluded_samples
    String sra_prep_version = "v0.1"
  }
}