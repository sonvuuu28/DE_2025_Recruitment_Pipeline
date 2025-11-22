import os
from uuid import UUID
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from cassandra.util import datetime_from_uuid1

from Cassandra import Cassandra
from MySql import MySql

# ===========================================================
# SPARK CONFIG
# ===========================================================

MYSQL_JAR = os.path.abspath("../driver/mysql-connector-j-8.0.33.jar")

spark = (
    SparkSession.builder.config(
        "spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0"
    )
    .config("spark.cassandra.connection.host", "cassandra_dl")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.driver.extraClassPath", MYSQL_JAR)
    .config("spark.executor.extraClassPath", MYSQL_JAR)
    .getOrCreate()
)

# DB Instances
cass = Cassandra(spark)
mysql = MySql(spark)

# ===========================================================
#      UDF
# ===========================================================


@udf(returnType=StringType())
def extract_timestamp_from_uuid(uuid_str):
    try:
        u = UUID(uuid_str)
        dt = datetime_from_uuid1(u)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


# ===========================================================
#      UTILITY READER
# ===========================================================


def spark_read_file(path):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return spark.read.csv(os.path.join(base_dir, path), header=True)


# ===========================================================
#              DATA TRANSFORM PIPELINE
# ===========================================================


class DataTransformer:
    valid_events = ["click", "conversion", "qualified", "unqualified"]

    @staticmethod
    def preprocess(df):
        # Get timestamp from uuid
        df = df.withColumn("system_ts", extract_timestamp_from_uuid(col("create_time")))
        df = df.withColumn(
            "system_ts", to_timestamp("system_ts", "yyyy-MM-dd HH:mm:ss")
        )

        # Select useful rows and cols
        df = df.select(
            "create_time",
            "system_ts",
            "job_id",
            "custom_track",
            "bid",
            "campaign_id",
            "group_id",
            "publisher_id",
        ).filter("job_id IS NOT NULL AND custom_track IS NOT NULL")

        return df

    @staticmethod
    def aggregate(df):
        df = df.filter(col("custom_track").isin(DataTransformer.valid_events))

        # Split dates and hours from timestamp
        df = df.withColumn("dates", to_date("system_ts"))
        df = df.withColumn("hours", hour("system_ts"))

        pivot_df = (
            df.groupBy(
                "job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id"
            )
            .pivot("custom_track", DataTransformer.valid_events)
            .agg(count("*").alias("count"))
        )

        # Rename {event}_count → event
        for e in DataTransformer.valid_events:
            pivot_df = pivot_df.withColumnRenamed(f"{e}_count", e)

        # Spend & bid
        metric_df = df.groupBy("job_id", "publisher_id", "campaign_id", "group_id").agg(
            round(sum("bid"), 2).alias("spend_hour"),
            round(avg("bid"), 2).alias("bid_set"),
        )

        return pivot_df.join(
            metric_df, ["job_id", "publisher_id", "campaign_id", "group_id"], "left"
        )

    @staticmethod
    def fill_null(df):
        fill_values = {
            "click": 0,
            "conversion": 0,
            "qualified": 0,
            "unqualified": 0,
            "spend_hour": 0,
            "bid_set": 0,
        }
        return df.fillna(fill_values)

    @staticmethod
    def post_process(df):
        # Add Primary Key
        df = df.withColumn("id", monotonically_increasing_id())

        # Mark update dates
        df = df.withColumn("updated_at", current_timestamp())
        return df.select(
            "id",
            "job_id",
            "dates",
            "hours",
            "company_id",
            "group_id",
            "campaign_id",
            "publisher_id",
            "click",
            "conversion",
            "qualified",
            "unqualified",
            "bid_set",
            "spend_hour",
            "sources",
            "updated_at",
        )

    @staticmethod
    def transform_full(df):
        df = DataTransformer.preprocess(df)
        df = DataTransformer.aggregate(df)
        df = df.withColumn("sources", lit("Cassandra"))
        df = DataTransformer.fill_null(df)

        job_df = mysql.read("job").select(col("id").alias("job_id"), "company_id")
        df = df.join(job_df, "job_id", "left")

        return DataTransformer.post_process(df)


# ===========================================================
#              SYNC CHECK BETWEEN MYSQL & CASS
# ===========================================================


class DataSync:

    @staticmethod
    def last_mysql_date():
        df = mysql.read("event")
        return df.select(max("updated_at")).first()[0]

    @staticmethod
    def last_cassandra_date():
        df = cass.read("tracking")
        df = df.withColumn("create_time", extract_timestamp_from_uuid("create_time"))
        df = df.withColumn("create_time", to_timestamp("create_time"))
        df = df.withColumn(
            "create_time", from_utc_timestamp("create_time", "Asia/Ho_Chi_Minh")
        )
        return df.select(max("create_time")).first()[0]


# ===========================================================
#                       MAIN ETL
# ===========================================================


def run_etl():
    df = cass.read("tracking")
    df = DataTransformer.transform_full(df)
    mysql.insert("event", df)


# ===========================================================
#                       ENTRY POINT
# ===========================================================

if __name__ == "__main__":
    # Initial load
    print("Insert Cassandra")
    cass.insert("tracking", spark_read_file("../data/cassandra/tracking.csv"))

    print("Insert MySQL")
    mysql.insert("job", spark_read_file("../data/mysql/job.csv"))

    # First ETL
    run_etl()

    # Continuous sync
    while True:
        if DataSync.last_mysql_date() < DataSync.last_cassandra_date():
            run_etl()
