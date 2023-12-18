version 1.0

task version_capture {
  meta {
    volatile: true
  }
  input {
    String? timezone
  }
  command {
    ttools_version="Terra Tools v0.1"
    ~{default='' 'export TZ=' + timezone}
    date +"%Y-%m-%d" > TODAY
    echo "$cbird_version" > TOOLS_VERSION
    date +"%Y%m%dT%H%M%S" > DATETIME
  }
  output {
    String date = read_string("TODAY")
    String ttools_version = read_string("TOOLS_VERSION")
    String timestamp = read_string("DATETIME")
  }
  runtime {
    memory: "1 GB"
    cpu: 1
    docker: "kincekara/jq:1.6"
    disks: "local-disk 10 HDD"
    dx_instance_type: "mem1_ssd1_v2_x2" 
  }
}