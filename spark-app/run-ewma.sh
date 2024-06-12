docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.executor.instances=3" \
  /opt/spark-app/ewma.py
