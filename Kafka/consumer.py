import json
import csv
from kafka import KafkaConsumer
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Uploade data to HDFS").getOrCreate()


if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        'new2',
        bootstrap_servers='dmk:9092',
        auto_offset_reset='earliest'
    )
    i = 1
    data = []
    for idx,message in enumerate(consumer):
        message = json.loads(message.value)
        data.append(message)
        if idx > 0 and idx%10 == 0:
            df = pd.DataFrame(data)
            data = []
            df_spark = spark.createDataFrame(df)
            df_spark.coalesce(1).write.mode('append').json('hdfs://master:9000/Data0')
            print("Upload success")
