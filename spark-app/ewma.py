from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, unix_timestamp
from pyspark.sql.types import FloatType, StructType, StructField, StringType
from datetime import datetime, timedelta
import requests
import json

# API 엔드포인트
read_api_url = "http://34.168.247.75:8080/get-ewma-data"
write_api_url = "http://34.168.247.75:8080/update-ewma-data"

# 지수 가중 이동 평균 계산 함수
def calculate_ewma(previous_ewma, new_value, alpha=0.1):
    if previous_ewma is None:
        return new_value
    return alpha * new_value + (1 - alpha) * previous_ewma

# UDF로 등록
ewma_udf = udf(calculate_ewma, FloatType())

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("WikiDataProcessing") \
    .getOrCreate()

# 데이터 스키마 정의
schema = StructType([
    StructField("doc_id", StringType(), True),
    StructField("previous_ewma", FloatType(), True),
    StructField("edit_count", FloatType(), True)
])

# 최근 1일간 데이터를 읽어오는 함수
def get_recent_data():
    response = requests.get(read_api_url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# API에서 데이터 읽기
recent_data = get_recent_data()

# Spark DataFrame 생성
df = spark.createDataFrame(recent_data, schema)

# 새로운 EWMA 계산
df = df.withColumn("new_ewma", ewma_udf(col("previous_ewma"), col("edit_count")))

# 각 행을 API로 전송하는 함수
def send_to_api(row):
    data = {
        "doc_id": row.doc_id,
        "new_ewma": row.new_ewma
    }
    response = requests.post(write_api_url, json=data)
    if response.status_code != 200:
        print(f"Failed to send data for doc_id {row.doc_id}: {response.status_code}, {response.text}")

# 각 배치마다 데이터를 API로 전송하는 함수
def foreach_batch_function(df, epoch_id):
    df.foreach(send_to_api)

# 스트리밍 쿼리 시작
query = df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start()

query.awaitTermination()
