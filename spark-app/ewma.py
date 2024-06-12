from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, avg, sum as _sum
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("WikiEditAnalysis") \
    .getOrCreate()

# MySQL JDBC 설정
jdbc_url = "jdbc:mysql://your_db_host:3306/your_db_name"
db_properties = {
    "user": "your_db_user",
    "password": "your_db_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# DB에서 최근 1일간 데이터 읽기
query = """
SELECT doc_id, edited_at, previous_ema, edit_count
FROM wiki_edits
WHERE edited_at >= NOW() - INTERVAL 1 DAY
"""

wiki_edits_df = spark.read.jdbc(url=jdbc_url, table=f"({query}) as wiki_edits", properties=db_properties)

# 지수가중이동평균(EMA) 계산
def calculate_ema(df, alpha=0.1):
    window_spec = Window.partitionBy("doc_id").orderBy("edited_at").rowsBetween(-1, 0)
    df = df.withColumn("ema", when(col("previous_ema").isNull(), col("edit_count"))
                        .otherwise(alpha * col("edit_count") + (1 - alpha) * col("previous_ema")))
    return df

wiki_edits_df = calculate_ema(wiki_edits_df)

# DB에 결과 저장
wiki_edits_df.write.jdbc(url=jdbc_url, table="wiki_edits", mode="append", properties=db_properties)

# 데이터 프레임 내용 출력 (디버깅 용도)
wiki_edits_df.show()

# Spark 세션 종료
spark.stop()
