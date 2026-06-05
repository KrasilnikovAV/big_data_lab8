import argparse

from mysql import MySQLClient, add_mysql_arguments, env, mysql_config_from_args
from pyspark.sql.functions import col, regexp_replace, trim
from schema_config import load_schema_config
from spark_config import add_spark_arguments, build_spark_with_jdbc

SCHEMA = load_schema_config()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load the prepared OpenFoodFacts dataset into MySQL."
    )
    add_mysql_arguments(parser)
    add_spark_arguments(parser)
    parser.add_argument(
        "--input",
        default=env("OPENFOODFACTS_INPUT", "data/sample_openfoodfacts.csv"),
    )
    parser.add_argument("--sep", default=env("OPENFOODFACTS_SEP", "\t"))
    parser.add_argument(
        "--target-table",
        default=env("MYSQL_SOURCE_TABLE", "product_nutrition"),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark_with_jdbc(
        app_name="OpenFoodFactsMySQLLoader",
        master=args.spark_master,
        jdbc_jar=args.jdbc_jar,
        config_path=args.spark_config,
    )
    mysql = MySQLClient(mysql_config_from_args(args))

    try:
        source_df = (
            spark.read.option("header", True)
            .option("sep", args.sep)
            .csv(args.input)
        )
        dataset_df = source_df.select(
            trim(col("code")).alias("product_code"),
            trim(col("product_name")).alias("product_name"),
            trim(col("brands")).alias("brands"),
            *[
                regexp_replace(col(f"`{source_name}`"), ",", ".")
                .cast("double")
                .alias(target_name)
                for source_name, target_name in SCHEMA["numeric_mappings"]
            ],
        )

        mysql.overwrite_table(dataset_df, args.target_table)
        print("Loaded source rows:", source_df.count())
        print("Rows written to MySQL:", dataset_df.count())
        print("Target table:", args.target_table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
