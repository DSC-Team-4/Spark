from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, IntegerType


# Spark 세션 생성
spark = SparkSession.builder \
    .appName("KafkaToJSON") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Kafka에서 읽기
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "34.47.86.209:9093") \
    .option("subscribe", "wiki") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Kafka 메시지의 value는 기본적으로 바이너리 형식이므로 문자열로 변환
kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")

# 데이터 스키마 정의
schema = StructType([
    StructField("$schema", StringType(), True),
    StructField("meta", StructType([
        StructField("uri", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True)
    ]), True),
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("notify_url", StringType(), True),
    StructField("minor", BooleanType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("length", StructType([
        StructField("old", IntegerType(), True),
        StructField("new", IntegerType(), True)
    ]), True),
    StructField("revision", StructType([
        StructField("old", LongType(), True),
        StructField("new", LongType(), True)
    ]), True),
    StructField("server_url", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("server_script_path", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("parsedcomment", StringType(), True)
])

# JSON 파싱 및 컬럼 추출
parsed_df = kafka_df.withColumn("data", from_json(col("json_string"), schema)).select("data.*")

# 데이터 확인을 위해 일부 출력 (60초 제한)
parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination(60)

# JSON 파일로 저장
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/opt/spark-app/output") \
    .option("checkpointLocation", "/opt/spark-app/checkpoint") \
    .start()

# 스트리밍 쿼리 종료
query.stop()

# 실제 파일로 저장하는 코드 추가
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/opt/spark-app/output") \
    .option("checkpointLocation", "/opt/spark-app/checkpoint") \
    .start()

# 50초 대기
query.awaitTermination(50)

# # 스트리밍 쿼리 종료
# query.stop()

# # Spark 세션 종료
# spark.stop()
