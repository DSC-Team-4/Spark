from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import requests

db_url = "http://34.168.247.75:8080/save"
kafka_url = "34.47.86.209:9093"
tags = ["User", "Wikipedia", "File", "MediaWiki", "Template", "Help", "Category", "Portal", "Draft", "TimedText", "Module", "Book", "Course", "Institution", "Education Program", "Gadget", "Gadget definition", "Topic", "Special"]

# API에 데이터를 전송하는 함수 정의
def send_to_api(row):
    if not row.title or not row.editedAt or not row.country or not row.uri:
        return
    temp = row.title.split(':',maxsplit=1)
    if temp[0].strip() in tags:
        return

    data = {
        "title": row.title,
        "editedAt": row.editedAt.isoformat(),
        "country": row.country,
        "uri": row.uri
    }
    response = requests.post(db_url, json=data)
    print(f"Response status: {response.status_code}, Response body: {response.text}")

# 각 배치마다 데이터를 API로 전송
def foreach_batch_function(df, epoch_id):
    df.foreach(send_to_api)

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("KafkaToDB") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()


# Kafka에서 읽기
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_url) \
    .option("kafka.value.deserializer.encoding", "UTF-8") \
    .option("subscribe", "wiki") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Kafka 메시지의 value는 기본적으로 바이너리 형식이므로 문자열로 변환
kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

# # 데이터 스키마 정의
schema = StructType([
    StructField("meta", StructType([
        StructField("uri", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("domain", StringType(), True)
    ]), True),
    StructField("title", StringType(), True)
])

# JSON 파싱 및 컬럼 추출
# parsed_df = kafka_df.withColumn("data", from_json(col("json_string"), schema)).select("data.*")

# JSON 데이터 파싱
parsed_df = kafka_df.select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.title").alias("title"),
        to_timestamp(col("data.meta.dt"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("editedAt"),
        col("data.meta.domain").alias("country"),
        col("data.meta.uri").alias("uri")
    )

# 데이터 확인을 위해 일부 출력 (30초 제한)
parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination(30)

# 스트리밍 쿼리 시작
query = parsed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start()

query.awaitTermination()

# # JSON 파일로 저장
# query = parsed_df.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "/opt/spark-app/output") \
#     .option("checkpointLocation", "/opt/spark-app/checkpoint") \
#     .start()

# # 50초 대기
# query.awaitTermination(50)

# # 스트리밍 쿼리 종료
# query.stop()

# # Spark 세션 종료
# spark.stop()