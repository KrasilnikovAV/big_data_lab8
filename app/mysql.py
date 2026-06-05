import os
from dataclasses import dataclass


def env(name, default):
    return os.environ.get(name, default)


def default_mysql_url():
    return "jdbc:mysql://mysql:3306/big_data_lab8?allowPublicKeyRetrieval=true&useSSL=false"


@dataclass
class MySQLConfig:
    url: str
    user: str
    password: str
    driver: str


def add_mysql_arguments(parser):
    parser.add_argument("--mysql-url", default=env("MYSQL_URL", default_mysql_url()))
    parser.add_argument("--mysql-user", default=env("MYSQL_USER", "lab8"))
    parser.add_argument("--mysql-password", default=env("MYSQL_PASSWORD", "lab8"))
    parser.add_argument(
        "--mysql-driver",
        default=env("MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver"),
    )


def mysql_config_from_args(args):
    return MySQLConfig(
        url=args.mysql_url,
        user=args.mysql_user,
        password=args.mysql_password,
        driver=args.mysql_driver,
    )


class MySQLClient:
    def __init__(self, config):
        self.config = config

    def options(self):
        return {
            "url": self.config.url,
            "user": self.config.user,
            "password": self.config.password,
            "driver": self.config.driver,
        }

    def read_table(self, spark, table_name):
        return (
            spark.read.format("jdbc")
            .options(**self.options())
            .option("dbtable", table_name)
            .load()
        )

    def append_table(self, df, table_name):
        (
            df.write.format("jdbc")
            .options(**self.options())
            .option("dbtable", table_name)
            .mode("append")
            .save()
        )

    def overwrite_table(self, df, table_name):
        (
            df.write.format("jdbc")
            .options(**self.options())
            .option("dbtable", table_name)
            .mode("overwrite")
            .save()
        )
