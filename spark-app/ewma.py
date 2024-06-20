# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col
# from pyspark.sql.types import StringType, StructType, StructField, DoubleType, ArrayType, IntegerType, TimestampType
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry
# import requests
# import json

# # 외부 API URL
# fetch_api_url = "http://34.168.247.75:8080/get-ewma-data"
# update_api_url = "http://34.168.247.75:8080/update-ewma-data"

# # Spark 세션 생성
# spark = SparkSession.builder \
#     .appName("ExternalAPIFetchAndUpdate") \
#     .config("spark.executor.memory", "3g") \
#     .config("spark.driver.memory", "2g") \
#     .config("spark.memory.fraction", "0.8") \
#     .getOrCreate()

# # API 호출 함수 정의
# def fetch_data_from_api():
#     session = requests.Session()
#     retry = Retry(connect=3, backoff_factor=0.5)
#     adapter = HTTPAdapter(max_retries=retry)
#     session.mount('http://', adapter)
#     session.mount('https://', adapter)
#     try:
#         response = session.get(fetch_api_url)
#         if response.status_code == 200:
#             return response.json()
#         else:
#             return None
#     except Exception as e:
#         print(f"Error fetching data: {e}")
#         return None

# # UDF (User Defined Function) 정의
# @udf(StringType())
# def fetch_data():
#     return json.dumps(fetch_data_from_api())

# # 빈 데이터 프레임 생성 (데이터가 없는 상태에서 시작)
# schema = StructType([
#     StructField("metaId", StringType(), True),
#     StructField("beforeEwma", DoubleType(), True),
#     StructField("wikiInfos", ArrayType(StructType([
#         StructField("title", StringType(), True),
#         StructField("country", StringType(), True),
#         StructField("uri", StringType(), True),
#         StructField("editCount", IntegerType(), True),
#         StructField("editedAt", TimestampType(), True)
#     ])), True)
# ])

# empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# # 데이터 프레임에 UDF 적용하여 외부 API 데이터 가져오기
# df_with_api_data = empty_df.withColumn("api_data", fetch_data())

# # JSON 파싱 및 구조화
# def parse_json(data):
#     if data:
#         return json.loads(data)
#     else:
#         return None

# @udf(schema)
# def parse_api_data(api_data):
#     return parse_json(api_data)

# parsed_df = df_with_api_data.withColumn("parsed_data", parse_api_data(col("api_data"))).select("parsed_data.*")

# # EWMA 계산 함수
# @udf(DoubleType())
# def calculate_ewma_udf(beforeEwma, wikiInfos):
#     if wikiInfos:
#         edit_count = len(wikiInfos)
#         alpha = 0.2
#         return alpha * edit_count + beforeEwma
#     return beforeEwma

# # EWMA 계산
# ewma_df = parsed_df.withColumn("ewma", calculate_ewma_udf(col("beforeEwma"), col("wikiInfos")))

# # 로그 파일 작성 함수
# def log_ewma_calculations(df):
#     log_file_path = '/opt/spark-app/ewma.txt'
#     with open(log_file_path, 'a') as log:
#         for row in df.collect():
#             if row['wikiInfos']:
#                 log.write(f"Title: {row['wikiInfos'][-1]['title']}, EditCount: {len(row['wikiInfos'])}\n")
#         log.write(f'Complete DF: {df.collect()}\n')

# log_ewma_calculations(ewma_df)

# # 데이터 POST 함수 정의
# def send_updated_data_to_api(data):
#     try:
#         response = requests.post(update_api_url, json=data)
#         print(f"Response status: {response.status_code}, Response body: {response.text}")
#     except Exception as e:
#         print(f"Error sending data: {e}")

# # foreachBatch 함수 정의
# def foreach_batch_function(df, epoch_id):
#     updated_data = df.select("metaId", "ewma", "wikiInfos").collect()
#     data_to_post = []
#     for row in updated_data:
#         edit_count = row.wikiInfos[-1]["editCount"] if row.wikiInfos else 0
#         data_to_post.append({
#             "metaId": row.metaId,
#             "ewma": row.ewma,
#             "editCount": edit_count
#         })
#     send_updated_data_to_api(data_to_post)

# # 스트리밍 쿼리 시작
# query = ewma_df.writeStream \
#     .foreachBatch(foreach_batch_function) \
#     .start()

# query.awaitTermination()

# # Spark 세션 종료
# spark.stop()

# # # UDF로 API 호출
# # def update_api(df):
# #     updated_data = df.select("metaId", "ewma", "wikiInfos").collect()
# #     data_to_post = []
# #     for row in updated_data:
# #         edit_count = row.wikiInfos[-1]["editCount"] if row.wikiInfos else 0
# #         data_to_post.append({
# #             "metaId": row.metaId,
# #             "ewma": row.ewma,
# #             "editCount": edit_count
# #         })
# #     send_updated_data_to_api(data_to_post)

# # # foreachBatch 함수 정의
# # def foreach_batch_function(df, epoch_id):
# #     df.foreachPartition(update_api)

# # # 스트리밍 쿼리 시작
# # query = ewma_df.writeStream \
# #     .foreachBatch(foreach_batch_function) \
# #     .start()

# # query.awaitTermination()

# # updated_data = ewma_df.collect()
# # send_updated_data_to_api(updated_data)

# # # Spark 세션 종료
# # spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, ArrayType, IntegerType, TimestampType
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import requests
import json
import time

# 외부 API URL
fetch_api_url = "http://34.168.247.75:8080/get-ewma-data"
update_api_url = "http://34.168.247.75:8080/update-ewma-data"

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("ExternalAPIFetchAndUpdate") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# API 호출 함수 정의
def fetch_data_from_api():
    print('fetch data from api')
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    print('Fetch data from api')
    try:
        response = session.get(fetch_api_url, timeout=300)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except requests.exceptions.Timeout:
        print("The request timed out")
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# UDF (User Defined Function) 정의
@udf(StringType())
def fetch_data():
    print('fetch start')
    temp = json.dumps(fetch_data_from_api())
    print(f'Fetch Data: {temp}\n')
    return temp
    # return json.dumps(fetch_data_from_api())

# 빈 데이터 프레임 생성 (데이터가 없는 상태에서 시작)
schema = StructType([
    StructField("metaId", StringType(), True),
    StructField("beforeEwma", DoubleType(), True),
    StructField("wikiInfos", ArrayType(StructType([
        StructField("title", StringType(), True),
        StructField("country", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("editCount", IntegerType(), True),
        StructField("editedAt", TimestampType(), True)
    ])), True)
])

empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# JSON 파싱 및 구조화
def parse_json(data):
    if data:
        print('parse_json')
        return json.loads(data)
    else:
        return None

@udf(schema)
def parse_api_data(api_data):
    print('parse_api_data')
    return parse_json(api_data)

# EWMA 계산 함수
@udf(DoubleType())
def calculate_ewma_udf(beforeEwma, wikiInfos):
    print(f'calculate ewma udf: {wikiInfos}')
    if wikiInfos:
        edit_counts = len(wikiInfos)
        alpha = 0.2
        return alpha * edit_counts + beforeEwma
    return beforeEwma

# 로그 파일 작성 함수
def log_ewma_calculations(df):
    log_file_path = '/opt/spark-app/ewma.txt'
    with open(log_file_path, 'a') as log:
        for row in df.collect():
            if row['wikiInfos']:
                log.write(f"Title: {row['wikiInfos'][-1]['title']}, EditCount: {len(row['wikiInfos'])}\n")
        log.write(f'Complete DF: {df.collect()}\n')

# 데이터 POST 함수 정의
def send_updated_data_to_api(data):
    try:
        response = requests.post(update_api_url, json=data)
        print(f"Response status: {response.status_code}, Response body: {response.text}")
    except Exception as e:
        print(f"Error sending data: {e}")

# UDF로 API 호출
def update_api(df):
    updated_data = df.select("metaId", "ewma", "wikiInfos").collect()
    print(f'updated_data: {updated_data}')
    data_to_post = []
    for row in updated_data:
        edit_count = row.wikiInfos[-1]["editCount"] if row.wikiInfos else 0
        print(f'Edit Count: {edit_count}\n')
        data_to_post.append({
            "metaId": row.metaId,
            "ewma": row.ewma,
            "editCount": edit_count
        })
    send_updated_data_to_api(data_to_post)

# 주기적으로 데이터 가져오기 및 처리
while True:
    print('Start')
    # 데이터 프레임에 UDF 적용하여 외부 API 데이터 가져오기
    df_with_api_data = empty_df.withColumn("api_data", fetch_data())
    parsed_df = df_with_api_data.withColumn("parsed_data", parse_api_data(col("api_data"))).select("parsed_data.*")
    # print(f'Fetch Data: {parsed_df}\n')
    ewma_df = parsed_df.withColumn("ewma", calculate_ewma_udf(col("beforeEwma"), col("wikiInfos")))

    # 로그 파일 작성
    log_ewma_calculations(ewma_df)

    # 데이터 업데이트
    update_api(ewma_df)

    # 1분 대기
    time.sleep(300)

# Spark 세션 종료
spark.stop()
