version 1.0

task prep_csv {
  input {
  String table_name
  String workspace_name
  String project_name
  Array[String] sample_names
  String record_id_column_name
  String wgs_id_column_name
  }

  command <<<
    # download terra table
    python3 /scripts/export_large_tsv/export_large_tsv.py --project "~{project_name}" --workspace "~{workspace_name}" --entity_type ~{table_name} --tsv_filename ~{table_name}-data.tsv

    python3 <<CODE 
    import pandas as pd
    import numpy as np

    # convert table into dataframe
    tablename = "~{table_name}-data.tsv"
    table = pd.read_csv(tablename, delimiter='\t', header=0, dtype={"~{table_name}_id": 'str'}) # ensure sample_id is always a string)

    # selected samples only
    table = table[table["~{table_name}_id"].isin("~{sep='*' sample_names}".split("*"))]

    # prep csv for redcap data import
    redcap = pd.DataFrame(columns=["~{table_name}_id","record_id", "arln_specimen_id", "phl",	"wgs_status", "wgs_id",	
    "srr_number",	"bacterial_wgs_result",	"reglab_comment",	"srx_number", "wgs_date_id_created","wgs_date_put_on_sequencer", "wgs_date_sent_to_seqfac",
    "scheme"])
    
    redcap["~{table_name}_id"] = table["~{table_name}_id"]
    redcap = redcap.set_index("~{table_name}_id")

    table2 = table.set_index("~{table_name}_id")
    table2 = table2.rename(columns={"~{record_id_column_name}":"record_id","~{wgs_id_column_name}":"wgs_id", "SRA_id":"srr_number", "mlst":"bacterial_wgs_result","pubmlst_scheme":"scheme"})

    redcap.loc[:, ["record_id", "wgs_id", "srr_number", "bacterial_wgs_result", "scheme"]] = table2[["record_id", "wgs_id", "srr_number", "bacterial_wgs_result", "scheme"]]
    redcap["arln_specimen_id"] = redcap["record_id"]
    redcap.fillna({"phl":"CT", "wgs_status":"WGS Successful"}, inplace=True)
    # fix mslt schemes
    redcap.loc[redcap["scheme"] == "ecoli", "scheme"] = "Pasteur"
    redcap.loc[redcap["scheme"] == "ecoli_achtman_4", "scheme"] = "Achtman"
    redcap.loc[redcap["scheme"] == "abaumannii", "scheme"] = "Oxford"
    redcap.loc[redcap["scheme"] == "abaumannii_2", "scheme"] = "Pasteur"
    redcap["bacterial_wgs_result"] = np.where(redcap["bacterial_wgs_result"] != "No ST predicted", "ML" + redcap["bacterial_wgs_result"].astype(str) +  "_" + redcap["scheme"], '')
    redcap.drop(columns=["scheme"], inplace=True)

    # write df
    redcap.to_csv("HAIAR_WGS_Data.csv", float_format='%.0f', index=False)  
    CODE
  >>>

  output {
    File redcap_input = "HAIAR_WGS_Data.csv"
  }

  runtime {
    docker: "us-docker.pkg.dev/general-theiagen/theiagen/terra-tools:2023-03-16"
    memory: "256 MB"
    cpu: 1
    disks: "local-disk 10 SSD"
    preemptible: 0
  }
}
