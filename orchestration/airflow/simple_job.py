import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

input_path = 'hdfs://localhost:9000/tmp/airflow/in/input.csv'
output_path = 'hdfs://localhost:9000/tmp/airflow/out/'

videos = spark.read.csv(path=input_path, header=True)

videos = (
    videos
    .groupBy('title', 'year')
    .agg(F.collect_set(F.col('tag')).alias('tags'))
    .orderBy(['title', 'year'])
    .repartition(1)
)

videos.write.mode('overwrite').format('avro').save(output_path)