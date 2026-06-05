from config_loader import load_key_value_config
from pyspark.sql import SparkSession
from mysql import env


DEFAULT_SPARK_MASTER = "local[2]"
DEFAULT_SPARK_CONFIG_PATH = "config/spark-defaults.conf"


def load_spark_config(path):
    return load_key_value_config(path, "Spark config")


def add_spark_arguments(parser):
    parser.add_argument("--spark-master", default=env("SPARK_MASTER", DEFAULT_SPARK_MASTER))
    parser.add_argument("--jdbc-jar", default=env("MYSQL_CONNECTOR_JAR", ""))
    parser.add_argument(
        "--spark-config",
        default=env("SPARK_CONFIG_PATH", DEFAULT_SPARK_CONFIG_PATH),
    )


def build_spark_session(app_name, master=None, config_path=DEFAULT_SPARK_CONFIG_PATH, extra_config=None):
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    spark_config = load_spark_config(config_path)
    if extra_config:
        spark_config.update(extra_config)

    for key, value in spark_config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def build_spark_with_jdbc(app_name, master=None, jdbc_jar="", config_path=DEFAULT_SPARK_CONFIG_PATH):
    extra_config = {"spark.jars": jdbc_jar} if jdbc_jar else None
    return build_spark_session(
        app_name=app_name,
        master=master,
        config_path=config_path,
        extra_config=extra_config,
    )
