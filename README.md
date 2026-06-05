# Big Data Lab 8: Migration to Kubernetes

Лабораторная работа 8 переносит контур из лабораторных 5-7 в Kubernetes:

- сервис модели KMeans на PySpark;
- сервис источника данных с загрузкой OpenFoodFacts в MySQL;
- сервис витрины данных на Scala + Spark;
- Spark-вычисления через native Spark on Kubernetes.

## Архитектура

```text
source-loader Spark Job -> MySQL/product_nutrition
datamart Spark Job      -> MySQL/dm_product_nutrition, dm_refresh_log
model Spark Job         -> MySQL/dm_kmeans_clusters
```

MySQL запускается как `StatefulSet` с PVC. Spark job'ы запускаются через
`spark-submit --master k8s://https://kubernetes.default.svc --deploy-mode cluster`.
Для вычислений создаются отдельные driver/executor pod'ы, а submitter pod остается
Kubernetes `Job`.

## Состав проекта

- `app/load_openfoodfacts_to_mysql.py` - источник данных, загрузка CSV в MySQL.
- `src/main/scala/DataMartApp.scala` - витрина данных на Scala/Spark.
- `app/kmeans_datamart.py` - модель KMeans на PySpark.
- `k8s/` - Kubernetes-манифесты.
- `scripts/k8s_build_image.sh` - сборка Docker-образа.
- `scripts/k8s_deploy.sh` - последовательный деплой контура.
- `scripts/k8s_verify.sh` - проверка таблиц MySQL после запуска.
- `build_distribution.sh` - сборка zip-дистрибутива.

## Запуск в Kubernetes

Нужен Kubernetes-кластер с доступом `kubectl` и локальный Docker. Для Minikube
скрипт сборки автоматически использует Docker daemon кластера.

```bash
./scripts/k8s_build_image.sh
./scripts/k8s_deploy.sh
./scripts/k8s_verify.sh
```

Ручной запуск:

```bash
kubectl apply -f k8s/00-namespace-rbac.yaml
kubectl apply -f k8s/01-mysql.yaml
kubectl wait --for=condition=ready pod/mysql-0 -n big-data-lab8 --timeout=10m
kubectl apply -f k8s/06-lab7-source-loader-job.yaml
kubectl wait --for=condition=complete job/source-loader-submit -n big-data-lab8 --timeout=20m
kubectl apply -f k8s/07-lab7-datamart-job.yaml
kubectl wait --for=condition=complete job/datamart-submit -n big-data-lab8 --timeout=20m
kubectl apply -f k8s/08-lab7-model-job.yaml
kubectl wait --for=condition=complete job/model-submit -n big-data-lab8 --timeout=20m
```

## Проверка

```bash
kubectl exec -n big-data-lab8 statefulset/mysql -- \
  mysql -ulab8 -plab8 big_data_lab8 \
  -e "SELECT COUNT(*) FROM product_nutrition; SELECT COUNT(*) FROM dm_product_nutrition; SELECT COUNT(*) FROM dm_kmeans_clusters;"
```

Также можно посмотреть driver/executor pod'ы Spark:

```bash
kubectl get pods -n big-data-lab8
kubectl logs -n big-data-lab8 job/model-submit
```

## Сборка дистрибутива

```bash
./build_distribution.sh
```

Архив создается по пути:

```text
dist/big_data_lab8_kubernetes_model.zip
```

## Очистка

```bash
./scripts/k8s_clean.sh
```
