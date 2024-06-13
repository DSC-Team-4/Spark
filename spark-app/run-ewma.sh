# ./run-ewma.sh
docker exec -it spark-master-ewma spark-submit \
  --master spark://spark-master-ewma:7077 \
  /opt/spark-app/ewma.py
