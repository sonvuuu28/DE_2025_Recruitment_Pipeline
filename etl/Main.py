from pyspark.sql import SparkSession
from Cassandra import Cassandra
from MySql import MySql
import os
from pyspark.sql.functions import *
from uuid import UUID
from cassandra.util import *

mysql_driver = os.path.abspath("../driver/mysql-connector-j-8.0.33.jar")


spark = (
    SparkSession.builder.config(
        "spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0"
    )
    .config("spark.driver.extraClassPath", mysql_driver)
    .config("spark.executor.extraClassPath", mysql_driver)
    .getOrCreate()
)

# Declare db
cass = Cassandra(spark)
mysql = MySql(spark)


def spark_read_file(path):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv = os.path.join(base_dir, path)
    df = spark.read.csv(csv, header=True)
    return df


@udf(returnType=StringType())
def extract_timestamp_from_uuid(uuid_str):
    try:
        u = UUID(uuid_str)
        dt = datetime_from_uuid1(u)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


def pre_processing_data(df):
    # Create timestamp based on uuid
    df = df.withColumn("system_ts", extract_timestamp_from_uuid(col("create_time")))

    # Select useful columns
    df = df.select(
        "create_time",
        "system_ts",
        "job_id",
        "custom_track",
        "bid",
        "campaign_id",
        "group_id",
        "publisher_id",
    )

    # Convert system_ts to timestamp
    df = df.withColumn(
        "system_ts", to_timestamp(col("system_ts"), "yyyy-MM-dd HH:mm:ss")
    )

    # Select useful rows
    df = df.filter(col("job_id").isNotNull())
    df = df.filter(col("custom_track").isNotNull())

    return df


def process_data(df):
    # Get neccessary event types
    valid_events = ["click", "conversion", "qualified", "unqualified"]
    df = df.filter(col("custom_track").isin(valid_events))

    # Split data and hour
    df = df.withColumn("dates", to_date(col("system_ts"))).withColumn(
        "hours", hour(col("system_ts"))
    )

    # Using pivot to advoid multiple joins
    result_df = (
        df.groupBy(
            "job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id"
        )
        .pivot("custom_track", valid_events)
        .agg(
            count("*").alias("count"),
        )
    )

    # Rename columns
    for event in valid_events:
        result_df = result_df.withColumnRenamed(f"{event}_count", event)

    # Calculate metrics
    agg_df = df.groupBy("job_id", "publisher_id", "campaign_id", "group_id").agg(
        round(sum("bid"), 2).alias("spend_hour"), round(avg("bid"), 2).alias("bid_set")
    )

    result_df = result_df.join(
        agg_df, on=["job_id", "publisher_id", "campaign_id", "group_id"], how="left"
    )

    return result_df


def replace_null(df):
    return df.fillna(
        {
            "click": 0,
            "conversion": 0,
            "qualified": 0,
            "unqualified": 0,
            "spend_hour": 0,
            "bid_set": 0,
        }
    )


def post_process(df):
    # Add Primary key
    df = df.withColumn("id", monotonically_increasing_id())

    # Mark time for update
    df = df.withColumn("updated_at", current_timestamp())

    df = df.select(
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
    )
    return df


def my_transform(df):
    # Prepocessing
    processed_df = pre_processing_data(df)

    # Calculate metrics
    final_df = process_data(processed_df)

    # Final output
    final_df = final_df.withColumn("sources", lit("Cassandra"))

    # Fill null
    final_df = replace_null(final_df)

    # Join job to get company_id
    job_df = mysql.read("job")
    job_df = job_df.select(col("id").alias("job_id"), "company_id")
    final_df = final_df.join(job_df, on="job_id", how="left")

    # Post process
    final_df = post_process(final_df)

    return final_df


def date_from_mysql():
    df = mysql.read("campaign")
    a = df.select(max("updated_at").alias("max")).collect()[0]
    return a["max"]


def date_from_cassandra():
    df = cass.read("tracking")
    df = df.withColumn("create_time", extract_timestamp_from_uuid(col("create_time")))
    df = df.withColumn(
        "create_time", to_timestamp("create_time", "yyyy-MM-dd HH:mm:ss")
    )
    df = df.withColumn(
        "create_time",
        from_utc_timestamp("create_time", "Asia/Ho_Chi_Minh"),  # đổi sang giờ VN
    )
    a = df.select(max("create_time").alias("max")).collect()[0]
    return a["max"]


if __name__ == "__main__":
    # Insert data to data lake
    print("insert Cassandra")
    tracking_df = spark_read_file("../data/cassandra/tracking.csv")
    cass.insert("tracking", tracking_df)

    # Insert data to data warehouse
    print("insert MySQL")
    job_df = spark_read_file("../data/mysql/job.csv")
    mysql.insert("job", job_df)

    # read data from Cassandra
    df = cass.read("tracking")

    # Transfrom data
    df = my_transform(df)

    # Insert data to data warehouse
    mysql.insert("campaign", df)

    while True:
        if date_from_mysql() < date_from_cassandra():
            # read data from Cassandra
            df = cass.read("tracking")

            # Transfrom data
            df = my_transform(df)

            # Insert data to data warehouse
            mysql.insert("campaign", df)
