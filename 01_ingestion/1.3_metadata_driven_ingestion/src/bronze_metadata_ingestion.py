import yaml
from pathlib import Path

def load_config(config_path):

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def validate_metadata(table):

    required_fields = [
        "name",
        "source",
        "target",
        "format"
    ]

    for field in required_fields:
        if field not in table:
            raise ValueError(
                f"Missing required field {field}"
            )

def create_table(spark, target):

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target}
        USING DELTA
    """)



def copy_into(spark, source, target, file_format):

    spark.sql(f"""
        COPY INTO {target}
        FROM '{source}'
        FILEFORMAT = {file_format.upper()}
        FORMAT_OPTIONS (
            'header' = 'true'
        )
    """)


def ingestion(spark, config):

    for table in config["tables"]:

        validate_metadata(table)

        source = table["source"]
        target = table["target"]
        file_format = table["format"]

        create_table(
            spark,
            target
        )

        copy_into(
            spark,
            source,
            target,
            file_format
        )