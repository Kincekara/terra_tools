version 1.0

workflow sra_fastq_download {

  meta {
  description: "download fastqs from SRA"
  }

  input {
    String sra_id
    String? docker
  }

  call fasterq_dump {
    input:
      sra_id = sra_id,
      docker = docker
  }

  output {
    File read1 = fasterq_dump.read1
    File read2 = fasterq_dump.read2
  }
}

task fasterq_dump {
  input {
    String sra_id
    String docker = "kincekara/sratoolkit:3.0.5"
  }

  command <<<
    fasterq-dump --version | tee VERSION
    fasterq-dump ~{sra_id}
    pigz *.fastq 
  >>>

  output {
    String version = read_string("VERSION")
    File read1 = "~{sra_id}_1.fastq.gz"
    File read2 = "~{sra_id}_2.fastq.gz"
  }

  runtime {
    docker: docker
    memory: "8 GB"
    cpu: 4
    disks: "local-disk 100 SSD"
    preemptible: 1
    maxRetries: 3
    }
}


