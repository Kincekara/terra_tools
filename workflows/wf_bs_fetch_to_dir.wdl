version 1.0

import "../tasks/task_version.wdl" as version
import "../tasks/task_bs_fetch.wdl" as fetch

workflow bs_fetch_to_dir {

  meta {
  author: "Kutluhan Incekara"
  email: "kutluhan.incekara@ct.gov"
  description: "Basespace FastQ fetch to a directory"
  }

  input {
    String sample_name
    String basespace_sample_name
    String? basespace_sample_id
    String basespace_collection_id
    String api_server
    String access_token
    Directory target_dir_path
  }

  call version.version_capture {
    input:
  }

  call fetch.fetch_bs {
    input:
      sample_name = sample_name,
      basespace_sample_id = basespace_sample_id,
      basespace_sample_name = basespace_sample_name,
      basespace_collection_id = basespace_collection_id,
      api_server = api_server,
      access_token = access_token,
      target_dir_path = target_dir_path
  }

  output {
    String bs_fetch_to_dir_date = version_capture.version
    String bs_fetch_to_dir_version = version_capture.date
    File read1 = fetch_bs.read1
    File? read2 = fetch_bs.read2
  }
}