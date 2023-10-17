version 1.0

task prep_tables {
  input {
    String table_name
    String workspace_name
    String project_name
    File? input_table
    Array[String] sample_names
    String bioproject
    String gcp_bucket_uri
    String read1_column_name = "read1"
    String read2_column_name = "read2"
    String submission_id_column_name
    String organism_column_name
  }
  command <<<
    # when running on terra, comment out all input_table mentions
    python3 /scripts/export_large_tsv/export_large_tsv.py --project "~{project_name}" --workspace "~{workspace_name}" --entity_type ~{table_name} --tsv_filename ~{table_name}-data.tsv
    
    python3 <<CODE 
    import pandas as pd
    import numpy as np
    import os

    # set a function to remove NA values and return the cleaned table and a table of excluded samples
    def remove_nas(table, required_metadata):
      table.replace(r'^\s+$', np.nan, regex=True) # replace blank cells with NaNs 
      excluded_samples = table[table[required_metadata].isna().any(axis=1)] # write out all rows that are required with NaNs to a new table
      excluded_samples.set_index("~{table_name}_id", inplace=True) # convert the sample names to the index so we can determine what samples are missing what
      excluded_samples = excluded_samples[excluded_samples.columns.intersection(required_metadata)] # remove all optional columns so only required columns are shown
      excluded_samples = excluded_samples.loc[:, excluded_samples.isna().any()] # remove all NON-NA columns so only columns with NAs remain; Shelly is a wizard and I love her 
      table.dropna(subset=required_metadata, axis=0, how='any', inplace=True) # remove all rows that are required with NaNs from table

      return table, excluded_samples

    # read export table into pandas
    tablename = "~{table_name}-data.tsv"
    table = pd.read_csv(tablename, delimiter='\t', header=0, dtype={"~{table_name}_id": 'str'}) # ensure sample_id is always a string)

    # extract the samples for upload from the entire table
    table = table[table["~{table_name}_id"].isin("~{sep='*' sample_names}".split("*"))]

    # set required and optional metadata fields
    required_metadata = ["submission_id", "organism", "collection_date", "geo_loc_name", "sample_type"]
    optional_metadata = ["sample_title", "bioproject_accession", "attribute_package", "strain", "isolate", "host", "isolation_source", "altitude", "biomaterial_provider", "collected_by", "depth", "env_broad_scale", "genotype", "host_tissue_sampled", "identified_by", "lab_host", "lat_lon", "mating_type", "passage_history", "samp_size", "serotype", "serovar", "specimen_voucher", "temp", "description", "MLST"]
    # add a column for biosample package -- required for XML submission
    table["attribute_package"] = "Microbe.1.0"
    # create required columns from exixting data and populate common information
    table["submission_id"] = table["~{submission_id_column_name}"]
    table["organism"] = table["~{organism_column_name}"]
    table["isolate"] = table["~{submission_id_column_name}"]
    table["host"] = "Homo sapiens"
    table["geo_loc_name"] = "USA"
    table["sample_type"] = "whole organism"
    table["MLST"] = np.where(table["mlst"] != "No ST predicted", "ML" + table["mlst"].astype(str), '' )      
    table["library_ID"] = table["~{submission_id_column_name}"]
    table["title"] = "Illumina sequencing of " + table["submission_id"].astype(str)
    table["library_strategy"] = "WGS"
    table["library_source"] = "GENOMIC"
    table["library_selection"] = "RANDOM"
    table["library_layout"] = "paired"
    table["platform"] = "ILLUMINA"
    table["instrument_model"] = "Illumina MiSeq"
    table["design_description"] = "Illumina MiSeq (V2) paired-end 2x150 reads"
    table["filetype"] = "fastq"
  
    # sra metadata 
    sra_required = ["~{table_name}_id", "submission_id", "library_ID", "title", "library_strategy", "library_source", "library_selection", "library_layout", "platform", "instrument_model", "design_description", "filetype", "~{read1_column_name}"]
    sra_optional = ["~{read2_column_name}"]

    # combine all required fields into one array for easy removal of NaN cells
    required_fields = required_metadata + sra_required

    # remove required rows with blank cells from table
    table, excluded_samples = remove_nas(table, required_fields)
    with open("excluded_samples.tsv", "a") as exclusions:
      exclusions.write("Samples excluded for missing required metadata (will have empty values in indicated columns):\n")
    excluded_samples.to_csv("excluded_samples.tsv", mode='a', sep='\t')

    # add bioproject_accesion to table
    table["bioproject_accession"] = "~{bioproject}"
    
     # extract the required metadata from the table
    biosample_metadata = table[required_metadata].copy()

    # add optional metadata fields if present; rename first column
    for column in optional_metadata:
      if column in table.columns:
        biosample_metadata[column] = table[column]
    biosample_metadata.rename(columns={"submission_id" : "sample_name"}, inplace=True)

    # extract the required metadata from the table; rename first column 
    sra_metadata = table[sra_required].copy()
    for column in sra_optional:
      if column in table.columns:
        sra_metadata[column] = table[column]
    sra_metadata.rename(columns={"submission_id" : "sample_name"}, inplace=True)

    # prettify the filenames and rename them to be sra compatible
    sra_metadata["~{read1_column_name}"] = sra_metadata["~{read1_column_name}"].map(lambda filename: filename.split('/').pop())
    sra_metadata.rename(columns={"~{read1_column_name}" : "filename"}, inplace=True)
    table["~{read1_column_name}"].to_csv("filepaths.tsv", index=False, header=False) # make a file that contains the names of all the reads so we can use gsutil -m cp
    if "~{read2_column_name}" in sra_metadata.columns:
      sra_metadata["~{read2_column_name}"] = sra_metadata["~{read2_column_name}"].map(lambda filename2: filename2.split('/').pop())   
      sra_metadata.rename(columns={"~{read2_column_name}" : "filename2"}, inplace=True)
      table["~{read2_column_name}"].to_csv("filepaths.tsv", mode='a', index=False, header=False)
    
    # write metadata tables to tsv output files
    biosample_metadata.to_csv("biosample_table.tsv", sep='\t', index=False)
    sra_metadata.to_csv("sra_table_to_edit.tsv", sep='\t', index=False)

    CODE

    # prune the first two columns of sra_table_to_edit to remove the tablename_id and submission_id columns
    cut -f3- sra_table_to_edit.tsv > sra_table.tsv

    # copy the raw reads to the bucket specified by user
    export CLOUDSDK_PYTHON=python2.7  # ensure python 2.7 for gsutil commands
    # iterate through file created earlier to grab the uri for each read file
    while read -r line; do
      echo "running \`gsutil -m cp ${line} ~{gcp_bucket_uri}\`"
      gsutil -m cp -n ${line} ~{gcp_bucket_uri}
    done < filepaths.tsv
    unset CLOUDSDK_PYTHON   # probably not necessary, but in case I do more things afterwards, this resets that env var

  >>>
  output {
    File biosample_table = "biosample_table.tsv"
    File sra_table = "sra_table.tsv"
    File sra_table_for_biosample = "sra_table_to_edit.tsv"
    File excluded_samples = "excluded_samples.tsv"
  }
  runtime {
    docker: "us-docker.pkg.dev/general-theiagen/theiagen/terra-tools:2023-03-16"
    memory: "8 GB"
    cpu: 4
    disks: "local-disk 100 SSD"
    preemptible: 0
  }
}
