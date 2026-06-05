CREATE DATABASE IF NOT EXISTS big_data_lab8;
USE big_data_lab8;

CREATE TABLE IF NOT EXISTS product_nutrition (
  product_code VARCHAR(64),
  product_name VARCHAR(255),
  brands VARCHAR(255),
  energy_kcal_100g DOUBLE,
  fat_100g DOUBLE,
  carbohydrates_100g DOUBLE,
  sugars_100g DOUBLE,
  proteins_100g DOUBLE,
  salt_100g DOUBLE
);

CREATE TABLE IF NOT EXISTS dm_product_nutrition (
  product_code VARCHAR(64),
  product_name VARCHAR(255),
  brands VARCHAR(255),
  energy_kcal_100g DOUBLE NOT NULL,
  fat_100g DOUBLE NOT NULL,
  carbohydrates_100g DOUBLE NOT NULL,
  sugars_100g DOUBLE NOT NULL,
  proteins_100g DOUBLE NOT NULL,
  salt_100g DOUBLE NOT NULL,
  source_table VARCHAR(128) NOT NULL,
  refreshed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS dm_refresh_log (
  run_id VARCHAR(80) NOT NULL,
  source_table VARCHAR(128) NOT NULL,
  mart_table VARCHAR(128) NOT NULL,
  rows_in_source BIGINT NOT NULL,
  rows_in_mart BIGINT NOT NULL,
  dropped_rows BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (run_id)
);

CREATE TABLE IF NOT EXISTS dm_kmeans_clusters (
  run_id VARCHAR(80) NOT NULL,
  source_table VARCHAR(128) NOT NULL,
  k INT NOT NULL,
  seed INT NOT NULL,
  sample_fraction DOUBLE NOT NULL,
  rows_from_mart BIGINT NOT NULL,
  rows_for_training BIGINT NOT NULL,
  silhouette DOUBLE NOT NULL,
  model_path VARCHAR(500) NOT NULL,
  cluster_id INT NOT NULL,
  cluster_count BIGINT NOT NULL,
  avg_energy_kcal_100g DOUBLE,
  avg_fat_100g DOUBLE,
  avg_carbohydrates_100g DOUBLE,
  avg_sugars_100g DOUBLE,
  avg_proteins_100g DOUBLE,
  avg_salt_100g DOUBLE,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (run_id, cluster_id)
);
