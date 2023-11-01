version 1.0

task prep_tables {
  input {
    String table_name
    String workspace_name
    String project_name
    Array[String] sample_names
    String bioproject
    String gcp_bucket_uri
    String submission_id_column_name
    String organism_column_name
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

    # prep Microbe 1.0
    microbe = pd.DataFrame(columns=["~{table_name}_id","*sample_name","sample_title","bioproject_accession","*organism", "strain","isolate","host","isolation_source","*collection_date",\
    "*geo_loc_name","*sample_type","altitude","biomaterial_provider","collected_by","culture_collection","depth","env_broad_scale","genotype",\
    "host_tissue_sampled","identified_by","lab_host","lat_lon","mating_type","passage_history","samp_size","serotype","serovar","specimen_voucher",\
    "temp","description","MLST"])

    microbe["~{table_name}_id"] = table["~{table_name}_id"]
    microbe = microbe.set_index("~{table_name}_id")

    table2 = table.set_index("~{table_name}_id")
    table2 = table2.rename(columns={"~{submission_id_column_name}":"*sample_name","~{organism_column_name}":"*organism","collection_date":"*collection_date","mlst":"MLST" })

    microbe.loc[:, ["*sample_name","*organism","isolation_source","*collection_date","MLST"]] = table2[["*sample_name","*organism","isolation_source","*collection_date","MLST"]]
    microbe.fillna({"bioproject_accession":"~{bioproject}", "host":"Homo sapiens", "*geo_loc_name":"USA", "*sample_type":"whole organism"}, inplace=True)
    microbe["isolate"] = microbe["*sample_name"]
    microbe["MLST"] = np.where(microbe["MLST"] != "No ST predicted", "ML" + microbe["MLST"].astype(str), '')
    microbe["*organism"] = microbe["*organism"].str.split(n=2).str[:2].str.join(" ")

    # prep sra_metadata
    sra_meta = pd.DataFrame(columns=["~{table_name}_id", "sample_name", "library_ID", "title", "library_strategy", "library_source", "library_selection", "library_layout", "platform", "instrument_model", "design_description", "filetype", "filename", "filename2"])
    sra_meta["~{table_name}_id"] = table["~{table_name}_id"] 
    sra_meta = sra_meta.set_index("~{table_name}_id")

    table2["read1"] = table2["read1"].map(lambda filename: filename.split('/').pop())
    table2["read2"] = table2["read2"].map(lambda filename: filename.split('/').pop())
    table2 = table2.rename(columns={"*sample_name":"sample_name", "read1":"filename" , "read2":"filename2"})
    sra_meta.loc[:, ["sample_name","filename","filename2"]] = table2[["sample_name","filename","filename2"]]
    sra_meta["library_ID"] = sra_meta["sample_name"]
    sra_meta["title"] = "Illumina sequencing of " + sra_meta["sample_name"].astype(str)
    sra_meta.fillna({"library_strategy":"WGS","library_source":"GENOMIC","library_selection":"RANDOM","library_layout":"paired",\
                    "platform":"ILLUMINA","instrument_model":"Illumina MiSeq","design_description":"Illumina MiSeq (V2) paired-end 2x150 reads",\
                    "filetype":"fastq"}, inplace=True)

    # generate a filepaths file for gsutil   
    table["read1"].to_csv("filepaths.tsv", index=False, header=False)
    table["read2"].to_csv("filepaths.tsv", mode='a', index=False, header=False)

    # write tables into files
    microbe.to_csv("microbe.tsv", sep='\t', float_format='%.0f', index=False)
    sra_meta.to_csv("sra_meta.tsv", sep='\t', index=False)

    CODE
    # iterate through file created earlier to grab the uri for each read file
    while read -r line; do
      echo "running \`gsutil -m cp ${line} ~{gcp_bucket_uri}\`"
      gsutil -m cp -n ${line} ~{gcp_bucket_uri}
    done < filepaths.tsv

  >>>
  output {
    File biosample_table = "microbe.tsv"
    File sra_table = "sra_meta.tsv"
  }

  runtime {
    docker: "us-docker.pkg.dev/general-theiagen/theiagen/terra-tools:2023-03-16"
    memory: "8 GB"
    cpu: 4
    disks: "local-disk 100 SSD"
    preemptible: 0
  }
}