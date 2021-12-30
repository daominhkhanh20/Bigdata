import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import pandas as pd 
spark = SparkSession.builder.appName("Uploade data to HDFS").getOrCreate()

def serializer(message):
    return json.dumps(message).encode('utf-8')


if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        'SmallData',
        bootstrap_servers='dmk:9092',
        auto_offset_reset='earliest',
    )
    i = 1
    data = []
    print("\n")
    for idx,message in enumerate(consumer):
        message = json.loads(message.value)
        data.append(message)
        # print(message)
        if idx >0 and idx%1000 == 0:
            df = pd.DataFrame(data)
            data = []
            df_spark = spark.createDataFrame(df)
            df_spark.coalesce(1).write.mode('append').json('hdfs://dmk:9000/DataVnExpress1')
            print("\n\nUpload success\n\n")
