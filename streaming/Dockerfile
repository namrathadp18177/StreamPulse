FROM bitnami/spark:3.5.1
WORKDIR /app
USER root
COPY anomaly_detector.py .
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "anomaly_detector.py"]
