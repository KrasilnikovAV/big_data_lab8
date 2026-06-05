FROM eclipse-temurin:17-jdk-jammy AS sbt-builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash curl ca-certificates git unzip zip \
    && rm -rf /var/lib/apt/lists/*

COPY build.sbt ./
COPY project ./project
COPY src ./src

RUN curl -fsSL https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt \
      -o /usr/local/bin/sbt \
    && chmod +x /usr/local/bin/sbt \
    && sbt assembly

FROM apache/spark-py:v3.4.0

USER root
WORKDIR /app

ARG MYSQL_CONNECTOR_VERSION=8.4.0

ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    MYSQL_CONNECTOR_JAR=/opt/spark/jars/mysql-connector-j.jar

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3-numpy zip \
    && curl -fsSL \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
      -o "${MYSQL_CONNECTOR_JAR}" \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p outputs target/scala-2.12 \
    && chmod -R 777 /app

COPY --from=sbt-builder /build/target/scala-2.12/big-data-lab8-k8s-datamart.jar /app/target/scala-2.12/big-data-lab8-k8s-datamart.jar
COPY . .

RUN chmod +x scripts/*.sh

CMD ["scripts/run_model.sh"]
