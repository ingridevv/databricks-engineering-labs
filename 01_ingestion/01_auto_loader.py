# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 01 - Autoloader 
# MAGIC #### Overview
# MAGIC Learn how Auto Loader performs incremental ingestion
# MAGIC using CloudFiles while understanding why each feature exists.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab I should be able to explain:
# MAGIC
# MAGIC ✅ How Auto Loader discovers files
# MAGIC
# MAGIC ✅ How checkpoints provide idempotency
# MAGIC
# MAGIC ✅ Why duplicate rows can still occur
# MAGIC
# MAGIC ✅ How schema evolution works
# MAGIC
# MAGIC ✅ What rescued data is
# MAGIC
# MAGIC ✅ How schema hints work
# MAGIC
# MAGIC ✅ Production best practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Setup
# MAGIC - Imports
# MAGIC - Config
# MAGIC - Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG engineering_labs;
# MAGIC USE SCHEMA 01_bronze;

# COMMAND ----------

import sys

# Derive the workspace path from the current user so this cell is portable
_current_user = spark.sql("SELECT current_user()").collect()[0][0]
_repo_path = f"/Workspace/Users/{_current_user}/databricks-engineering-labs"
sys.path.insert(0, _repo_path)
from config.settings import *  # type: ignore[import-not-found]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Lab

# COMMAND ----------

def reset_lab(table_name: str, checkpoint: str):
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.{table_name}")
    print(f"Table {table_name} is dropped.")

    dbutils.fs.rm(f"{CHECKPOINT_PATH}/{checkpoint}", recurse=True)
    dbutils.fs.rm(SCHEMA_PATH, recurse=True)

    dbutils.fs.mkdirs(CHECKPOINT_PATH)
    dbutils.fs.mkdirs(SCHEMA_PATH)
    print("Checkpoint and schema subfolders created successfully.")

# COMMAND ----------

reset_lab(
    table_name="patients",
    checkpoint="patients"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### utils.autoloader 

# COMMAND ----------

# DBTITLE 1,run_autoloader
def run_autoloader(
    file_name: str,
    table_name: str,
    checkpoint: str,
    file_format: str = "csv",
    reader_options: dict | None = None,
    writer_options: dict | None = None
):
    # Reader
    reader = (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", file_format)
                .option("header", "true")
                .option("rescuedDataColumn", "_rescued_data")
                .option("cloudFiles.schemaLocation", 
                        f"{SCHEMA_PATH}/{table_name}"
                        )
                .option("cloudFiles.inferColumnTypes", "true")
                .option("pathGlobFilter", file_name)
    )

    # Read Stream
    if reader_options:
        for key, value in reader_options.items():
            reader = reader.option(key, value)

    df = reader.load(RAW_PATH)

    # Write to stream
    writer = (
        df.writeStream
            .trigger(availableNow=True)
            .option(
                "checkpointLocation",
                f"{CHECKPOINT_PATH}/{checkpoint}"
            )
    )

    if writer_options:
        for key, value in writer_options.items():
            writer = writer.option(key, value)

    query = writer.toTable(f"{BRONZE_SCHEMA}.{table_name}")

    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 01 - Initial Ingestion
# MAGIC - cloudFiles
# MAGIC - schemaLocation
# MAGIC - checkpoints
# MAGIC - availableNow
# MAGIC
# MAGIC **Expected outcome**:
# MAGIC
# MAGIC - Auto Loader infers schema.
# MAGIC - Creates checkpoint.
# MAGIC - Loads 1163 rows.

# COMMAND ----------

run_autoloader(
    file_name="patients.csv",
    table_name="patients",
    checkpoint="patients",
    file_format="csv"
)

# COMMAND ----------

df_patients = spark.sql("""SELECT count(*) FROM `01_bronze`.patients;    
                        """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Auto Loader Metadata
# MAGIC checkpoints/
# MAGIC - patients/
# MAGIC     - commits/
# MAGIC     - offsets/
# MAGIC     - sources/
# MAGIC     - metadata/

# COMMAND ----------

display(dbutils.fs.ls(f"{SCHEMA_PATH}/patients/_schemas"))

# COMMAND ----------

display(dbutils.fs.ls(f"{CHECKPOINT_PATH}/patients"))

# COMMAND ----------

# DBTITLE 1,Read the schema version 0
print(
    dbutils.fs.head(
        f"{SCHEMA_PATH}/patients/_schemas/0"
    )
)

# COMMAND ----------

# DBTITLE 1,Inspect commits
dbutils.fs.head(
    f"{CHECKPOINT_PATH}/patients/commits/0"
)

# COMMAND ----------

# DBTITLE 1,List offsets
display(
    dbutils.fs.ls(
        f"{CHECKPOINT_PATH}/patients/offsets"
    )
)

# COMMAND ----------

# DBTITLE 1,Inspect offsets
print(
    dbutils.fs.head(
        f"{CHECKPOINT_PATH}/patients/offsets/0"
    )
)

# COMMAND ----------

# DBTITLE 1,Inspect log from unitystorage
print(
    dbutils.fs.head(
        f"{CHECKPOINT_PATH}/patients/sources/0/rocksdb/logs/000004-3d55656d-2688-42e7-83e1-f818cf93b95a.log"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 02 - File Idempotency
# MAGIC Demonstrates that Auto Loader performs idempotent ingestion by tracking
# MAGIC processed files through checkpoints.
# MAGIC
# MAGIC **Expected behavior:**
# MAGIC - patients.csv is processed once.
# MAGIC - Re-running the same ingestion does not duplicate data.
# MAGIC - patients_new.csv is considered a new file and is ingested once.
# MAGIC - Re-running patients_new.csv does not ingest it again because its path is already recorded in the checkpoint.
# MAGIC
# MAGIC **Engineering concept:**
# MAGIC
# MAGIC Auto Loader guarantees file-level idempotency, not record-level deduplication.

# COMMAND ----------

run_autoloader(
    file_name="patients_new.csv",
    table_name="patients",
    checkpoint="patients",
    file_format="csv"
)

# COMMAND ----------

incremental_ingestion_df = spark.sql("""
                                     SELECT COUNT(*) FROM `01_bronze`.patients
                                     """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 03 - Schema Evolution
# MAGIC
# MAGIC Set `cloudFiles.schemaEvolutionMode` options:
# MAGIC
# MAGIC - Default behavior (addNewColumns)
# MAGIC - Fail On New Columns
# MAGIC - Rescue Mode
# MAGIC - Type Widening
# MAGIC - None

# COMMAND ----------

# DBTITLE 1,Default Behavior (addNewColumns)
run_autoloader(
    file_name="patients_schema_change.csv",
    table_name="patients",
    checkpoint="patients",
    file_format="csv",
    writer_options={
        "mergeSchema": "true"
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation**
# MAGIC
# MAGIC The default `addNewColumns` mode intentionally stops the stream when a new
# MAGIC column is detected.
# MAGIC
# MAGIC On the first execution:
# MAGIC
# MAGIC - Schema metadata is updated.
# MAGIC - The stream terminates.
# MAGIC - `automatic_retry=True`
# MAGIC
# MAGIC Result:
# MAGIC
# MAGIC - Auto Loader recognizes the updated schema.
# MAGIC - The DataFrame is created successfully.
# MAGIC - Delta Lake applies the schema evolution (`mergeSchema=true`).

# COMMAND ----------

df_schema_change = spark.read.table("patients")
df_schema_change.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation**
# MAGIC
# MAGIC The `failOnNewColumns` mode intentionally stops the stream when a new
# MAGIC column is detected, and do not evolve schema.
# MAGIC
# MAGIC **Expected**:
# MAGIC - Detect unknown column.
# MAGIC - Stop ingestion.
# MAGIC - Do not evolve schema.
# MAGIC - Require manual intervention.
# MAGIC
# MAGIC **Result**:
# MAGIC - Stream failed.
# MAGIC - Unknown field detected.
# MAGIC - `automatic retry: false`
# MAGIC - Schema metadata was not updated.

# COMMAND ----------

# DBTITLE 1,Fail on new columns
run_autoloader(
    file_name="patients_schema_change.csv",
    table_name="patients",
    checkpoint="patients",
    reader_options={
        "cloudFiles.schemaEvolutionMode": "failOnNewColumns"
    }
)

# COMMAND ----------

# DBTITLE 1,Type Widening
run_autoloader(
    file_name="patients_type_widening.csv",
    table_name="patients",
    checkpoint="patients",
    reader_options={
        "cloudFiles.schemaEvolutionMode": "addNewColumnsWithTypeWidening"
    }
)

# COMMAND ----------

df_type_widening = spark.read.table("patients")
df_type_widening.printSchema()

# COMMAND ----------

# DBTITLE 1,None
run_autoloader(
    file_name="patients_schema_change.csv",
    table_name="patients",
    checkpoint="patients",
    reader_options={
        "cloudFiles.schemaEvolutionMode": "None"
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 04 - Rescued Data

# COMMAND ----------

# DBTITLE 1,Rescue
run_autoloader(
    file_name="patients_schema_change.csv",
    table_name="patients",
    checkpoint="patients",
    reader_options={
        "cloudFiles.schemaEvolutionMode": "rescue"
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation**
# MAGIC
# MAGIC Schema `rescue` preserves unexpected columns inside `_rescued_data`, but it does not update or merge existing records. Record deduplication and reconciliation belong in the Silver layer.
# MAGIC
# MAGIC **Expected**:
# MAGIC - Preserve unknown columns inside `_rescued_data`.
# MAGIC - Do not evolve the table schema.
# MAGIC - Continue ingestion without failing.
# MAGIC - Preserve all incoming records.
# MAGIC
# MAGIC **Result**:
# MAGIC - Stream completed successfully.
# MAGIC - `HEALTHCARE_PROVIDER` was stored in `_rescued_data`.
# MAGIC - The table schema remained unchanged.
# MAGIC - 1,164 rows contained non-null `_rescued_data`.
# MAGIC - Existing patient records were not updated or merged; a new version of each record was appended.
# MAGIC
# MAGIC **Takeaway:***
# MAGIC `rescue` prioritizes data preservation over schema evolution. Instead of rejecting unexpected fields or modifying the table schema, Auto Loader safely stores unknown attributes in `_rescued_data`, allowing downstream pipelines to decide how and when those fields should be incorporated.

# COMMAND ----------

df_schema_change = spark.sql(""" 
                            SELECT id, _rescued_data
                            FROM patients
                            WHERE _rescued_data IS NOT NULL
                            LIMIT 5;
                             """).show(truncate=False)

# COMMAND ----------

df_schema_count = spark.sql("""
                            SELECT
                            COUNT(*) AS rescued_rows
                            FROM patients
                            WHERE _rescued_data IS NOT NULL;
                            """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 05 - Schema Hints
# MAGIC
# MAGIC **Observation**
# MAGIC
# MAGIC `cloudFiles.schemaHints` is only applied during the initial schema inference.
# MAGIC
# MAGIC Once Auto Loader has persisted the inferred schema in `schemaLocation`, subsequent executions reuse that schema and ignore new schema hints.
# MAGIC
# MAGIC **Expected:**
# MAGIC - Apply the specified data types during schema inference.
# MAGIC - Override automatic type inference for the specified columns.
# MAGIC - Persist the inferred schema for future ingestions.
# MAGIC
# MAGIC **Result:**
# MAGIC - Schema hints were successfully applied after resetting the lab.
# MAGIC - `ZIP` was inferred as `STRING`.
# MAGIC - `BIRTHDATE` was inferred as `STRING`.
# MAGIC - The inferred schema was stored in `schemaLocation`.
# MAGIC - Future ingestions reused the stored schema instead of inferring it again.
# MAGIC
# MAGIC **Takeaway**
# MAGIC
# MAGIC Schema hints are not schema migration tools. They influence only the initial schema inference. If a schema has already been persisted in `schemaLocation`, the stored schema takes precedence over new hints. To test different schema hints, the schema metadata must be reset or a new `schemaLocation` must be used.

# COMMAND ----------

# DBTITLE 1,Schema Hints
run_autoloader(
    file_name="patients_schema_hints.csv",
    table_name="patients",
    checkpoint="patients",
    reader_options={
        "cloudFiles.schemaHints": "ZIP STRING, BIRTHDATE STRING"
    }
)

# COMMAND ----------

df_schema_hints = spark.read.table("patients")
df_schema_hints.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Considerations
# MAGIC
# MAGIC - Never store checkpoints inside DBFS root.
# MAGIC
# MAGIC - Store checkpoints separately from raw data.
# MAGIC
# MAGIC - One checkpoint per stream.
# MAGIC
# MAGIC - Bronze should preserve raw records.
# MAGIC
# MAGIC - Deduplication belongs in Silver.
# MAGIC
# MAGIC - Auto Loader tracks files, not records.
# MAGIC
# MAGIC - Schema evolution should be intentional.
# MAGIC
# MAGIC ###  Takeaways
# MAGIC - ✓ Auto Loader watches directories, not files.
# MAGIC - ✓ availableNow terminates the stream.
# MAGIC - ✓ schemaLocation stores inferred schemas.
# MAGIC - ✓ checkpointLocation stores ingestion progress.
# MAGIC - ✓ DBFS is disabled in Free Edition.
# MAGIC - ✓ Unity Catalog Volumes require /Volumes/catalog/schema/volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC - [What is Auto Loader? (Databricks Docs)](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
# MAGIC - [How Does Auto Loader Schema Evolution Works (Databricks Docs)](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#how-does-auto-loader-schema-evolution-work)