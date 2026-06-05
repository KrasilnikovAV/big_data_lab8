# Отчет по лабораторной работе №8

## Тема

Миграция сервисов Spark/PySpark и источника данных на Kubernetes.

## Цель работы

Получить навыки оркестрации контейнеров с использованием Kubernetes путем
миграции сервиса модели на PySpark, сервиса витрины на Spark и сервиса источника
данных.

## Исходная база

В качестве основы использованы результаты предыдущих лабораторных:

- лабораторная №5: модель KMeans на PySpark для данных OpenFoodFacts;
- лабораторная №6: источник данных MySQL и загрузчик CSV;
- лабораторная №7: витрина данных на Scala + Spark и обновленная модель,
  работающая с витринной таблицей.

В лабораторной №8 контур перенесен из `docker-compose` в Kubernetes.

## Реализованная инфраструктура Kubernetes

Созданы манифесты в каталоге `k8s/`:

- `00-namespace-rbac.yaml` - namespace `big-data-lab8`, `ServiceAccount` и RBAC
  для Spark driver pod'ов;
- `01-mysql.yaml` - `Secret`, SQL init `ConfigMap`, `Service` и `StatefulSet`
  MySQL 8.4 с постоянным томом;
- `02-spark-storage.yaml` - опциональный пример PVC для внешних Spark-артефактов;
- `06-lab7-source-loader-job.yaml` - Spark job загрузки исходного CSV в MySQL;
- `07-lab7-datamart-job.yaml` - Spark job построения витрины данных;
- `08-lab7-model-job.yaml` - Spark job обучения модели KMeans.

Spark запускается в native Kubernetes mode через:

```text
spark-submit --master k8s://https://kubernetes.default.svc --deploy-mode cluster
```

Каждый submitter создается как Kubernetes `Job`, а фактические вычисления
выполняются в Spark driver/executor pod'ах.

## Миграция сервисов

### Этап 1. Сервис модели из лабораторной №5

Модель KMeans сохранена как PySpark-приложение. Для Kubernetes добавлена
возможность не задавать `local[2]` внутри приложения: если `SPARK_MASTER`
передан пустым значением, master берется из `spark-submit`. Это необходимо,
чтобы Spark создавал executor pod'ы в Kubernetes, а не запускался локально
внутри driver pod'а.

### Этап 2. Источник данных из лабораторной №6

Источник данных перенесен в MySQL `StatefulSet`. Данные инициализируются через
SQL `ConfigMap`, учетные данные вынесены в Kubernetes `Secret`.

Загрузчик `source-loader-submit` читает `data/sample_openfoodfacts.csv` из Docker
образа и записывает таблицу `product_nutrition` через JDBC.

### Этап 3. Витрина данных из лабораторной №7

Scala/Spark-витрина запускается job'ом `datamart-submit`. Она читает
`product_nutrition`, очищает данные, строит таблицу `dm_product_nutrition` и
пишет журнал обновления в `dm_refresh_log`.

### Этап 4. Обновленная модель

Модель запускается job'ом `model-submit` и читает только
`dm_product_nutrition`. Результаты кластеризации записываются в
`dm_kmeans_clusters`. Локальные файлы модели в Kubernetes считаются временными,
а проверяемым артефактом является таблица результатов в MySQL.

## Репликация и ресурсы

Для Spark настроены отдельные executor pod'ы:

- загрузчик источника: `spark.executor.instances=1`;
- витрина: `spark.executor.instances=2`;
- модель: `spark.executor.instances=2`.

Для submitter pod'ов и MySQL заданы `requests` и `limits`. Настройки подобраны
для локального Kubernetes/Minikube:

- MySQL: request `250m CPU`, `512Mi`, limit `1 CPU`, `1Gi`;
- Spark submitter: request `100m CPU`, `256Mi`, limit `500m CPU`, `512Mi`;
- Spark driver/executor: `1 core`, `1g memory`;
- `spark.sql.shuffle.partitions=4` для уменьшения лишних shuffle-задач на малом
  датасете.

MySQL оставлен в одной реплике, так как это stateful-сервис с PVC. Репликация
вычислений обеспечивается executor pod'ами Spark, что соответствует характеру
нагрузки лабораторной.

## Порядок запуска

Сборка образа:

```bash
./scripts/k8s_build_image.sh
```

Деплой полного контура:

```bash
./scripts/k8s_deploy.sh
```

Скрипт последовательно:

1. применяет namespace, RBAC и MySQL;
2. ждет готовности `mysql-0`;
3. запускает `source-loader-submit`;
4. после завершения запускает `datamart-submit`;
5. после завершения запускает `model-submit`.

## Проверка работоспособности

Проверка выполняется командой:

```bash
./scripts/k8s_verify.sh
```

Скрипт выводит:

- количество строк в `product_nutrition`;
- количество строк в `dm_product_nutrition`;
- последние записи `dm_refresh_log`;
- последние результаты `dm_kmeans_clusters`.

Дополнительные команды:

```bash
kubectl get pods -n big-data-lab8
kubectl logs -n big-data-lab8 job/model-submit
kubectl exec -n big-data-lab8 statefulset/mysql -- mysql -ulab8 -plab8 big_data_lab8 -e "SHOW TABLES;"
```

## Артефакты

- Kubernetes-манифесты: `k8s/`
- Dockerfile: `Dockerfile`
- Скрипт деплоя: `scripts/k8s_deploy.sh`
- Скрипт проверки: `scripts/k8s_verify.sh`
- Архив дистрибутива: `dist/big_data_lab8_kubernetes_model.zip`

## Вывод

Цель лабораторной работы достигнута. Контур источника данных, витрины и модели
перенесен в Kubernetes. Spark-вычисления выполняются в native Kubernetes mode
с driver/executor pod'ами, MySQL работает как stateful-сервис, секреты вынесены
в Kubernetes `Secret`, а результаты модели доступны через таблицу
`dm_kmeans_clusters`.
