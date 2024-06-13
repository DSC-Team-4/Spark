# ./run-wiki.sh
sudo docker exec -it spark-master-wiki spark-submit \
  --master spark://spark-master-wiki:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-app/spark.py
