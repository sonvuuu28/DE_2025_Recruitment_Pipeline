from pyspark.sql.functions import *


# ============================
# Lớp Cassandra
# ============================
class Cassandra:
    # ----------------------------
    # Constructor
    # ----------------------------
    def __init__(self, spark):
        self.spark = spark

    # ----------------------------
    # Đọc dữ liệu từ bảng
    # ----------------------------
    def read(self, table):
        df = (
            self.spark.read.format("org.apache.spark.sql.cassandra")
            .options(keyspace="de_2025_datalake", table=table)
            .load()
        )
        return df

    # ---------------------------------------------------------------------
    # Ghi DataFrame vào bảng (overwrite), dùng cho lần ghi dữ liệu vào đầu tiên
    # ---------------------------------------------------------------------
    def insert(self, table, df):
        try:
            df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").options(
                **{
                    "keyspace": "de_2025_datalake",
                    "table": table,
                    "confirm.truncate": "true",  # Truncate & Load
                }
            ).save()
            return True

        except Exception as e:
            return False

    # ----------------------------------------
    # Ghi DataFrame vào bảng (append), dùng cho CDC
    # ----------------------------------------
    def insert_random(self, table, df):
        try:
            df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
                **{"keyspace": "de_2025_datalake", "table": table}
            ).save()
            return True

        except Exception as e:
            print("Insert random error:", e)
            return False
