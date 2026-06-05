import argparse
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from mysql import MySQLClient, add_mysql_arguments, env, mysql_config_from_args
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql.functions import avg, col, count, current_timestamp, lit
from schema_config import load_schema_config
from spark_config import add_spark_arguments, build_spark_with_jdbc

SCHEMA = load_schema_config()
FEATURE_COLUMNS = SCHEMA["feature_columns"]
META_COLUMNS = SCHEMA["model_meta_columns"]


class KMeansFromMartJob:
    def __init__(self, args, mysql):
        self.args = args
        self.mysql = mysql
        self.output_dir = Path(args.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = args.run_id or self.build_run_id()
        self.spark = None

    def run(self):
        self.spark = self.build_spark()
        try:
            mart_df = self.mysql.read_table(self.spark, self.args.source_table)
            prepared_df = self.prepare_training_rows(mart_df)
            rows_from_mart = prepared_df.count()
            training_df = self.sample_training_rows(prepared_df)
            rows_for_training = training_df.count()
            if rows_for_training < self.args.k:
                raise ValueError(
                    f"Need at least {self.args.k} rows from data mart, got {rows_for_training}."
                )

            model, predictions = self.train(training_df)
            silhouette = self.evaluate(predictions)
            cluster_stats = self.summarize(predictions)

            model_path = self.save_local_outputs(model, cluster_stats)
            metrics = self.build_metrics(
                rows_from_mart=rows_from_mart,
                rows_for_training=rows_for_training,
                silhouette=silhouette,
                model_path=model_path,
            )
            self.write_metrics_file(metrics)
            self.upload_results(cluster_stats, metrics)
            self.print_results(metrics, cluster_stats)
        finally:
            if self.spark is not None:
                self.spark.stop()

    def build_spark(self):
        return build_spark_with_jdbc(
            app_name="DataMartKMeans",
            master=self.args.spark_master,
            jdbc_jar=self.args.jdbc_jar,
            config_path=self.args.spark_config,
        )

    def prepare_training_rows(self, mart_df):
        missing = sorted(set(META_COLUMNS + FEATURE_COLUMNS) - set(mart_df.columns))
        if missing:
            raise ValueError(
                f"Data mart table '{self.args.source_table}' is missing columns: {', '.join(missing)}"
            )

        return mart_df.select(
            *(META_COLUMNS + [col(feature).cast("double").alias(feature) for feature in FEATURE_COLUMNS])
        )

    def train(self, training_df):
        pipeline = Pipeline(
            stages=[
                VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="raw_features"),
                StandardScaler(
                    inputCol="raw_features",
                    outputCol="features",
                    withStd=True,
                    withMean=True,
                ),
                KMeans(
                    k=self.args.k,
                    seed=self.args.seed,
                    featuresCol="features",
                    predictionCol="cluster_id",
                ),
            ]
        )
        model = pipeline.fit(training_df)
        return model, model.transform(training_df)

    def sample_training_rows(self, df):
        if self.args.sample_fraction >= 1.0:
            return df
        return df.sample(
            withReplacement=False,
            fraction=self.args.sample_fraction,
            seed=self.args.seed,
        )

    def evaluate(self, predictions):
        evaluator = ClusteringEvaluator(
            featuresCol="features",
            predictionCol="cluster_id",
            metricName="silhouette",
        )
        return evaluator.evaluate(predictions)

    def summarize(self, predictions):
        return (
            predictions.groupBy("cluster_id")
            .agg(
                count("*").alias("cluster_count"),
                *[avg(feature).alias(f"avg_{feature}") for feature in FEATURE_COLUMNS],
            )
            .orderBy("cluster_id")
        )

    def save_local_outputs(self, model, cluster_stats):
        model_path = self.output_dir / "model"
        self.remove_if_exists(model_path)
        model.write().overwrite().save(str(model_path))

        self.write_single_csv(cluster_stats, self.output_dir / "cluster_stats")
        return model_path

    def build_metrics(self, rows_from_mart, rows_for_training, silhouette, model_path):
        return {
            "run_id": self.run_id,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "mysql_url": self.args.mysql_url,
            "source_table": self.args.source_table,
            "results_table": self.args.results_table,
            "k": self.args.k,
            "seed": self.args.seed,
            "sample_fraction": self.args.sample_fraction,
            "features": FEATURE_COLUMNS,
            "rows_from_mart": rows_from_mart,
            "rows_for_training": rows_for_training,
            "silhouette": silhouette,
            "model_path": str(model_path),
        }

    def write_metrics_file(self, metrics):
        with (self.output_dir / "metrics.json").open("w", encoding="utf-8") as file:
            json.dump(metrics, file, indent=2)

    def upload_results(self, cluster_stats, metrics):
        results_df = cluster_stats
        for column_name, value in (
            ("run_id", metrics["run_id"]),
            ("source_table", metrics["source_table"]),
            ("k", metrics["k"]),
            ("seed", metrics["seed"]),
            ("sample_fraction", metrics["sample_fraction"]),
            ("rows_from_mart", metrics["rows_from_mart"]),
            ("rows_for_training", metrics["rows_for_training"]),
            ("silhouette", metrics["silhouette"]),
            ("model_path", metrics["model_path"]),
        ):
            results_df = results_df.withColumn(column_name, lit(value))

        results_df = results_df.withColumn("created_at", current_timestamp())
        self.mysql.append_table(results_df, self.args.results_table)

    def print_results(self, metrics, cluster_stats):
        print("Run ID:", metrics["run_id"])
        print("Source mart table:", metrics["source_table"])
        print("Rows from mart:", metrics["rows_from_mart"])
        print("Rows for training:", metrics["rows_for_training"])
        print("KMeans k:", metrics["k"])
        print("Silhouette:", round(metrics["silhouette"], 4))
        print("Model saved to:", metrics["model_path"])
        print("Results uploaded to MySQL table:", self.args.results_table)
        print("Cluster statistics:")
        cluster_stats.show(truncate=False)

    def write_single_csv(self, df, path):
        self.remove_if_exists(path)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path))

    def remove_if_exists(self, path):
        target = Path(path)
        if target.exists():
            shutil.rmtree(target)

    @staticmethod
    def build_run_id():
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{timestamp}-{uuid.uuid4().hex[:8]}"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train Spark ML KMeans on rows prepared by the Scala data mart."
    )
    add_mysql_arguments(parser)
    add_spark_arguments(parser)
    parser.add_argument("--source-table", default=env("MYSQL_SOURCE_TABLE", "dm_product_nutrition"))
    parser.add_argument("--results-table", default=env("MYSQL_RESULTS_TABLE", "dm_kmeans_clusters"))
    parser.add_argument("--output-dir", default=env("OUTPUT_DIR", "outputs/model_from_mart"))
    parser.add_argument("--k", type=int, default=int(env("KMEANS_K", "5")))
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=float(env("SAMPLE_FRACTION", "1.0")),
    )
    parser.add_argument("--seed", type=int, default=int(env("KMEANS_SEED", "42")))
    parser.add_argument("--run-id", default=env("RUN_ID", ""))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    KMeansFromMartJob(args, MySQLClient(mysql_config_from_args(args))).run()
