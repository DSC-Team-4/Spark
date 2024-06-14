from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import json
import uuid
import re

wiki_domains = {
    "en": "미국",
    "de": "독일",
    "fr": "프랑스",
    "es": "스페인",
    "it": "이탈리아",
    "nl": "네덜란드",
    "ru": "러시아",
    "ja": "일본",
    "zh": "중국",
    "ko": "한국",
    "vi": "베트남",
    "pt": "포르투갈",
    "pl": "폴란드",
    "sv": "스웨덴",
    "ar": "아랍권",
    "he": "이스라엘",
    "th": "태국",
    "id": "인도네시아",
    "fa": "이란",
    "tr": "터키",
    "uk": "우크라이나",
    "cs": "체코",
    "hu": "헝가리",
    "ro": "루마니아",
    "el": "그리스",
    "da": "덴마크",
    "fi": "핀란드",
    "no": "노르웨이",
    "sk": "슬로바키아",
    "bg": "불가리아",
    "hi": "인도",
    "bn": "방글라데시",
    "ms": "말레이시아",
    "lt": "리투아니아",
    "sl": "슬로베니아",
    "hr": "크로아티아",
    "et": "에스토니아",
    "lv": "라트비아",
    "ka": "조지아",
    "sq": "알바니아",
    "hy": "아르메니아",
    "az": "아제르바이잔",
    "eu": "바스크",
    "ga": "아일랜드",
    "is": "아이슬란드",
    "mk": "북마케도니아",
    "mt": "몰타",
    "sw": "케냐",
    "ta": "스리랑카",
    "tl": "필리핀",
    "ur": "파키스탄",
    "common": "공통"
}

db_url = "http://34.168.247.75:8080/save"
kafka_url = "34.47.86.209:9093"

def get_country_from_url(url):
    match = re.search(r'https?://(\w+)\.wikipedia\.org', url)
    if match:
        lang_code = match.group(1)
        return wiki_domains.get(lang_code, "알 수 없음")
    return "알 수 없음"

# API에 데이터를 전송하는 함수 정의
def send_to_api(row, log_file):
    if not row.title or not row.editedAt or not row.domain or not row.uri or not row.metaId or not row.namespace:
        return
    if row.namespace == 0 :
        log_file.write(f'namespace: {row.namespace}, title: {row.title}\n')
    if row.namespace != 0:
        log_file.write(f'{row.schema}, namespace: {row.namespace}, Not Zero!\n')
        return
    country = get_country_from_url(row.uri)
    log_file.write(f'country: {country}\n')
    if country == "알 수 없음":
        return
    log_file.write(f'From: {country}, Title: {row.title}\n')

    data = {
        "title": row.title,
        "editedAt": row.editedAt.isoformat(),
        "country": country,
        "uri": row.uri,
        "metaId": row.metaId
    }
    response = requests.post(db_url, json=data)
    log_file.write(f"Response status: {response.status_code}, Response body: {response.text}\n")
    log_file.flush()

# 각 배치마다 데이터를 API로 전송
def foreach_batch_function(df, epoch_id):
    with open('/opt/spark-app/output.txt', 'a') as log_file:
        for row in df.collect():
            send_to_api(row, log_file)

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("KafkaToDB") \
    .master("spark://spark-master-wiki:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.executor.cores", "8") \
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

# 데이터 스키마 정의
schema = StructType([
    StructField("$schema", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("meta", StructType([
        StructField("id", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("domain", StringType(), True)
    ]), True)
])

# JSON 데이터 파싱
parsed_df = kafka_df.select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.$schema").alias("schema"),
        col("data.title").alias("title"),
        col("data.namespace").alias("namespace"),
        to_timestamp(col("data.meta.dt"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("editedAt"),
        col("data.meta.domain").alias("domain"),
        col("data.meta.uri").alias("uri"),
        col("data.meta.id").alias("metaId")
    )

# 데이터 확인을 위해 일부 출력 (10초 제한)
# query = parsed_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination(10)

# 스트리밍 쿼리 시작
query = parsed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()

# 스트리밍 쿼리 종료
query.stop()

# Spark 세션 종료
# spark.stop()
