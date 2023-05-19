version 1.0

import "../tasks/task_sratoolkit.wdl" as sratools

workflow sra_fastq_download {

  meta {
  description: "download fastqs from SRA"
  }

  input {
    String sra_id
  }

  call sratools.fasterq_dump {
    input:
      sra_id = sra_id
  }

  output {
    File read1 = fasterq_dump.read1
    File read2 = fasterq_dump.read2
  }
}
