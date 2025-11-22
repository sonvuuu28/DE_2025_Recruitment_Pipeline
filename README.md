DE project: Recruitment Pipeline

[![Python](https://img.shields.io/badge/python-3.10-blue)](https://www.python.org/)  
[![Spark](https://img.shields.io/badge/spark-3.5.6-orange)](https://spark.apache.org/)  
[![Cassandra](https://img.shields.io/badge/Cassandra-latest-red)](https://cassandra.apache.org/)  
[![MySQL](https://img.shields.io/badge/MySQL-latest-lightgrey)](https://www.mysql.com/)  
[![Grafana](https://img.shields.io/badge/Grafana-latest-orange)](https://grafana.com/)  
[![Docker](https://img.shields.io/badge/Docker-latest-blue)](https://www.docker.com/)  
[![GitHub](https://img.shields.io/badge/GitHub-latest-black)](https://github.com/)

# Tổng quan dự án
### 1. Mục tiêu dự án
Xây pipeline Micro-Batch ETL near-real-time từ CSV tĩnh và API giả lập CDC: lưu dữ liệu thô vào Cassandra(Data Lake), transform bằng Spark + Python, load vào MySQL(Data Warehouse) và hiển thị trên Grafana.  Toàn bộ hệ thống được container hóa bằng Docker và triển khai trên một máy ảo VirtualBox, kèm CI/CD trên GitHub.
### 2. High Level Architecture
Dữ liệu đi từ Sources → Ingest → Data Lake (Cassandra) → ETL (Spark) → Data Warehouse (MySQL) → Consumers (Grafana / SQL / Jupyter). Tất cả components đều được đóng gói bằng docker và chạy trong VM đểdemo.
![alt text](image/hla.png)

### 3. Input/Output
Input Data: CSV, API python
Output: DashBoard (Grafana), và 1 pipeline tự động ETL, tất cả chạy hoàn toàn ở 1 máy ảo

Grafana:
![alt text](image/grafana.png)
---

Automatically ETL:
![ETL Demo](image/demo_etl.gif)

Server:


----
# I. Docker Preparation

```
docker/
├── config/
│   └── spark-defaults.conf
├── spark/
│   └── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
└── requirements.txt
```

Mục tiêu: Build image Spark riêng, Cassandra và MySQL dùng image từ Docker Hub.

---

## 1. Build Spark Image

Dockerfile:

```dockerfile
FROM python:3.10-bookworm as spark-base

# Cài tool cần thiết
RUN apt-get update && \
    apt-get install -y sudo curl vim unzip rsync openjdk-17-jdk build-essential software-properties-common ssh && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Thiết lập môi trường
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
RUN mkdir -p ${SPARK_HOME} ${HADOOP_HOME}
WORKDIR ${SPARK_HOME}

# Tải Spark
RUN curl https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz -o spark.tgz \
 && tar xvzf spark.tgz --strip-components 1 \
 && rm spark.tgz

# Cài Python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Config Spark
ENV PATH="$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV PYSPARK_PYTHON=python3
COPY config/spark-defaults.conf $SPARK_HOME/config
RUN chmod +x $SPARK_HOME/sbin/* $SPARK_HOME/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Entrypoint
COPY entrypoint.sh .
ENTRYPOINT ["./entrypoint.sh"]
```

Lưu ý:

* Spark chạy trong container, kết nối Cassandra / MySQL từ Docker Hub.
* `entrypoint.sh` chạy ETL tự động khi container start.

---

## 2. Docker Compose Setup
docker-compose:
```yaml
services:

  # Cassandra (Data Lake)
  cassandra:
    image: cassandra:4.1
    container_name: cassandra_dl
    ports:
      - "9042:9042"
    volumes:
      - cassandra-data:/var/lib/cassandra
    environment:
      CASSANDRA_CLUSTER_NAME: "first_cluster"
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe keyspaces'"]
      interval: 10s
      retries: 5
    networks:
      - de_project

  # MySQL (Data Warehouse)
  mysql:
    image: mysql:8.0.44-debian
    container_name: mysql_dwh
    ports:
      - "3307:3306"
    environment:
      MYSQL_ROOT_PASSWORD: 123
    volumes:
      - mysql-data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      retries: 5
    networks:
      - de_project

  # Spark Master
  spark-master:
    container_name: spark-engine
    build:
      context: .
      dockerfile: spark/Dockerfile
    image: da-spark-image
    entrypoint: ['./entrypoint.sh', 'master']
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 5s
      timeout: 3s
      retries: 3
    volumes:
      - ../data:/opt/spark/data
      - ../etl:/opt/spark/etl
      - spark-logs:/opt/spark/spark-events
    env_file:
      - .env
    ports:
      - "9090:8080"  # Web UI
      - "7077:7077"  # Spark master
      - "4041:4040"  # Spark driver UI
    networks:
      - de_project

  # Grafana (Monitoring)
  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
    ports:
      - "3000:3000"
    networks:
      - de_project

# Volumes
volumes:
  cassandra-data:
  mysql-data:
  spark-logs:

# Networks
networks:
  de_project:
    name: de_project
    driver: bridge
```

Giải thích:

* Cassandra → Data Lake, port 9042
* MySQL → Data Warehouse, port 3307
* Spark Master → chạy ETL, kết nối CSV/ETL code
* Grafana → Monitoring, port 3000
* Volumes → lưu dữ liệu persistent
* Network de_project → tất cả container cùng network nội bộ

---

# II. ETL Pipeline
```
├── 🐍 Cassandra.py
├── 🐍 Main.py
├── 🐍 MySql.py
└── 📄 generate_data_automatically.ipynb
```
Mục tiêu:
- Lấy dữ liệu thô từ Cassandra (Datalake)
- Transfrom ở Main
- Đưa dữ liệu vào MySQL (Data Warehouse)
- Tạo các bản ghi liên tục tự động đưa vào Datalake (📄 generate_data_automatically.ipynb)

### 1. Code
```python
import os
from uuid import UUID
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from cassandra.util import datetime_from_uuid1

from Cassandra import Cassandra
from MySql import MySql

# ======================================================================
#                         SPARK CONFIGURATION
# ======================================================================

# Đường dẫn MySQL Driver (để Spark có thể ghi vào MySQL)
MYSQL_JAR = os.path.abspath("../driver/mysql-connector-j-8.0.33.jar")

# Tạo SparkSession, kèm theo kết nối Cassandra + MySQL
spark = (
    SparkSession.builder.config(
        # Connector để Spark đọc Cassandra
        "spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0"
    )
    # Add MySQL driver
    .config("spark.driver.extraClassPath", MYSQL_JAR)
    .config("spark.executor.extraClassPath", MYSQL_JAR)
    .getOrCreate()
)

# Tạo instance DB để tương tác dễ dàng
cass = Cassandra(spark)
mysql = MySql(spark)

# ======================================================================
#                                UDF
# ======================================================================
# UDF này dùng để lấy timestamp thật từ UUID v1 trong Cassandra
# Cassandra lưu thời gian trong uuid1 → phải tự convert
@udf(returnType=StringType())
def extract_timestamp_from_uuid(uuid_str):
    try:
        u = UUID(uuid_str)
        dt = datetime_from_uuid1(u)  # Trích timestamp từ UUID
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


# ======================================================================
#                         DATA TRANSFORM PIPELINE
# ======================================================================
class DataTransformer:

    # Chỉ nhận các event hợp lệ
    valid_events = ["click", "conversion", "qualified", "unqualified"]

    # -------------------------------------------------------------
    # 1️⃣ Tiền xử lý dữ liệu
    # -------------------------------------------------------------
    @staticmethod
    def preprocess(df):
        # Chuyển create_time (UUID) → timestamp
        df = df.withColumn("system_ts", extract_timestamp_from_uuid(col("create_time")))
        df = df.withColumn("system_ts", to_timestamp("system_ts"))

        # Chọn cột cần thiết — loại bỏ cột rác
        return df.select(
            "create_time", "system_ts", "job_id", "custom_track", "bid",
            "campaign_id", "group_id", "publisher_id"
        ).filter("job_id IS NOT NULL AND custom_track IS NOT NULL")

    # -------------------------------------------------------------
    # 2️⃣ Tổng hợp dữ liệu theo Job / ngày / giờ / campaign
    # -------------------------------------------------------------
    @staticmethod
    def aggregate(df):
        # Chỉ giữ event hợp lệ
        df = df.filter(col("custom_track").isin(DataTransformer.valid_events))

        # Extract ngày & giờ
        df = df.withColumn("dates", to_date("system_ts"))
        df = df.withColumn("hours", hour("system_ts"))

        # Pivot để đếm mỗi loại event theo giờ
        pivot_df = (
            df.groupBy("job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id")
            .pivot("custom_track", DataTransformer.valid_events)
            .agg(count("*").alias("count"))
        )

        # Đổi tên cột click_count → click
        for e in DataTransformer.valid_events:
            pivot_df = pivot_df.withColumnRenamed(f"{e}_count", e)

        # Tính spend theo giờ + bid trung bình
        metric_df = df.groupBy("job_id", "publisher_id", "campaign_id", "group_id").agg(
            round(sum("bid"), 2).alias("spend_hour"),
            round(avg("bid"), 2).alias("bid_set"),
        )

        # JOIN lại để có bảng hoàn chỉnh
        return pivot_df.join(
            metric_df, ["job_id", "publisher_id", "campaign_id", "group_id"], "left"
        )

    # -------------------------------------------------------------
    # 3️⃣ Điền các cột null thành 0 để dễ lưu vào MySQL
    # -------------------------------------------------------------
    @staticmethod
    def fill_null(df):
        return df.fillna({
            "click": 0,
            "conversion": 0,
            "qualified": 0,
            "unqualified": 0,
            "spend_hour": 0,
            "bid_set": 0,
        })

    # -------------------------------------------------------------
    # 4️⃣ Tạo PK + timestamp cập nhật
    # -------------------------------------------------------------
    @staticmethod
    def post_process(df):
        df = df.withColumn("id", monotonically_increasing_id())
        df = df.withColumn("updated_at", current_timestamp())

        # Final schema cho bảng trong MySQL
        return df.select(
            "id", "job_id", "dates", "hours", "company_id", "group_id",
            "campaign_id", "publisher_id", "click", "conversion",
            "qualified", "unqualified", "bid_set", "spend_hour",
            "sources", "updated_at"
        )

    # -------------------------------------------------------------
    # 5️⃣ Pipeline đầy đủ
    # -------------------------------------------------------------
    @staticmethod
    def transform_full(df):
        df = DataTransformer.preprocess(df)
        df = DataTransformer.aggregate(df)
        df = df.withColumn("sources", lit("Cassandra"))
        df = DataTransformer.fill_null(df)

        # Lấy thêm thông tin công ty từ bảng job
        job_df = mysql.read("job").select(col("id").alias("job_id"), "company_id")
        df = df.join(job_df, "job_id", "left")

        return DataTransformer.post_process(df)


# ======================================================================
#                     CHECK SYNC MYSQL <-> CASSANDRA
# ======================================================================
class DataSync:

    # Lấy timestamp update cuối cùng trong MySQL
    @staticmethod
    def last_mysql_date():
        df = mysql.read("campaign")
        return df.select(max("updated_at")).first()[0]

    # Lấy timestamp mới nhất từ Cassandra
    @staticmethod
    def last_cassandra_date():
        df = cass.read("tracking")
        df = df.withColumn("create_time", extract_timestamp_from_uuid("create_time"))
        df = df.withColumn("create_time", to_timestamp("create_time"))
        df = df.withColumn(
            "create_time",
            from_utc_timestamp("create_time", "Asia/Ho_Chi_Minh")
        )
        return df.select(max("create_time")).first()[0]


# ======================================================================
#                             MAIN ETL
# ======================================================================
def run_etl():
    df = cass.read("tracking")
    df = DataTransformer.transform_full(df)
    mysql.insert("campaign", df)


# ======================================================================
#                           ENTRY POINT
# ======================================================================
if __name__ == "__main__":
    # Load dữ liệu mẫu vào DB
    print("Insert Cassandra")
    cass.insert("tracking", spark_read_file("../data/cassandra/tracking.csv"))

    print("Insert MySQL")
    mysql.insert("job", spark_read_file("../data/mysql/job.csv"))

    # Chạy ETL lần đầu
    run_etl()

    # Worker chạy liên tục → chỉ ETL khi có dữ liệu mới
    while True:
        if DataSync.last_mysql_date() < DataSync.last_cassandra_date():
            run_etl()
```
### 2. Kết quả đạt được 
![ETL Demo](image/demo_etl.gif)

Nhận xét: 
- Dữ liệu được xử lý tự động khi có bản ghi mới được đưa vào Datalake: Micro-Batch ETL

# III. Visualization (Grafana)
### 1. Config Mysql
```
Grafana → Connections → Data Sources → mysql 
```

![Grafana Config](image/grafana_config.png)

### 2. Kết quả đạt được
![alt text](image/grafana.png)


# IV. Server Preparation

### 1. Cài đặt VM

* Cài VirtualBox: [VirtualBox](https://www.virtualbox.org/wiki/Downloads)
* Tải Ubuntu Server: [Ubuntu](https://ubuntu.com/download/server)

---

### 2. Cấu hình server

#### 2.1 Tạo máy ảo

1. VirtualBox → New → Name: `Ubuntu_VM`
2. Type: Linux, Version: Ubuntu (64-bit)
3. RAM: 2–4 GB
4. Hard disk: 20GB+
5. Start VM → cài Ubuntu từ ISO

#### 2.2 Cấu hình mạng

![Config Image](image/image.png)

* Settings → Network → Adapter 1 → Port Forwarding
* Mở port SSH host → VM (ví dụ host port 2222 → guest port 22)

#### 2.3 Cài OpenSSH server

```bash
sudo apt update
sudo apt install openssh-server -y
sudo systemctl enable ssh
sudo systemctl start ssh
```

* Kiểm tra SSH từ host:

```bash
ssh <username>@<host_ip> -p <host_port>
```

![test\_ssh.png](image/test_ssh.png)

#### 2.4 Cài đặt hỗ trợ

```bash
sudo apt install git -y
sudo apt install docker.io docker-compose -y
```

# V. Deployment & CI/CD Pipeline
Mục đích: kéo code từ github về server và chạy ở server

```
git clone https://github.com/sonvuuu28/DE_2025_Recruitment_Pipeline.git
```
![git clone](image/gitclone.png)

```
sudo docker-compose up -d
```

![docker-compose up](image/docker-compose_up.png)











![alt text](image/docker_server.png)