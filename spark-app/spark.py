from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import json
import uuid
import re
from datetime import datetime
import pytz

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
    "commons": "공통"
}

db_url = "http://34.168.247.75:8080/save"
kafka_url = "34.47.86.209:9093"

def get_country_from_url(url):
    match = re.search(r'https?://(\w+)\.wikipedia\.org', url)
    if match:
        lang_code = match.group(1)
        return wiki_domains.get(lang_code, "알 수 없음")
    return "문서 아님"

# 한국 시간으로 변환하는 함수
def convert_to_korean_time(utc_time):
    utc_time = datetime.strptime(utc_time, '%Y-%m-%dT%H:%M:%S')
    utc_time = utc_time.replace(tzinfo=pytz.utc)
    korean_time = utc_time.astimezone(pytz.timezone('Asia/Seoul'))
    return korean_time.strftime('%Y-%m-%dT%H:%M:%S')

# API에 데이터를 전송하는 함수 정의
def send_to_api(row, log_file):
    if not row.title or not row.editedAt or not row.uri or not row.metaId or not row.kind or row.namespace:
        return
    if row.namespace == 0 and row.kind == 'edit':
        country = get_country_from_url(row.uri)
        if country == "문서 아님": return

        korean_time = convert_to_korean_time(row.editedAt.isoformat())
    
        log_file.write('=============================================\n')
        log_file.write(f'UTC Time: {row.editedAt.isoformat()}, Korean Time: {korean_time}, Title: {row.title}, From: {country}\n')
        log_file.write('=============================================\n')
        
        data = {
            "title": row.title,
            "editedAt": korean_time,
            "country": country,
            "uri": row.uri,
            "metaId": row.metaId
        }
        try:
            response = requests.post(db_url, json=data)
            log_file.write(f"Response status: {response.status_code}, Response body: {response.text}\n")
        except: pass
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
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# Kafka에서 읽기
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_url) \
    .option("kafka.value.deserializer.encoding", "UTF-8") \
    .option("subscribe", "wiki") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Kafka 메시지의 value는 기본적으로 바이너리 형식이므로 문자열로 변환
kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

# 데이터 스키마 정의
schema = StructType([
    StructField("page_change_kind", StringType(), True),
    StructField("page", StructType([
        StructField("page_id", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("namespace_id", IntegerType(), True)
    ]), True),
    StructField("dt", StringType(), True),
    StructField("meta", StructType([
        StructField("uri", StringType(), True),
    ]), True)
])

# JSON 데이터 파싱
parsed_df = kafka_df.select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.page_change_kind").alias("kind"),
        col("data.page.page_id").alias("metaId"),
        col("data.page.page_title").alias("title"),
        col("data.page.namespace_id").alias("namespace"),
        to_timestamp(col("data.dt"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("editedAt"),
        col("data.meta.uri").alias("uri"),
    )

# 데이터 확인을 위해 일부 출력 (10초 제한)
# query = parsed_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination(100)

# 스트리밍 쿼리 시작
query = parsed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()

# 스트리밍 쿼리 종료
# query.stop()

# Spark 세션 종료
# spark.stop()
