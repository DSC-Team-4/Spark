#!/bin/bash
# ./run-ewma.sh
/snap/bin/docker exec -i spark-master-ewma spark-submit \
  --master spark://spark-master-ewma:7077 \
  /opt/spark-app/ewma.py
