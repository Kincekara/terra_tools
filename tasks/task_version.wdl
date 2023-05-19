version 1.0

task version_capture {
  input {
    String? timezone
  }
  meta {
    volatile: true
  }
  command {
    version="v0.1-dev"
    ~{default='' 'export TZ=' + timezone}
    date +"%Y-%m-%d" > TODAY
    echo "$version" > VERSION
  }
  output {
    String date = read_string("TODAY")
    String version = read_string("VERSION")
  }
  runtime {
    memory: "1 GB"
    cpu: 1
    docker: "kincekara/bash:alpine"
    disks: "local-disk 10 HDD"
    dx_instance_type: "mem1_ssd1_v2_x2" 
  }
}