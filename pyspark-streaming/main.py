from pyspark.sql import SparkSession
from constants import cols, source_params
import data_processor
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
    .appName('pyspark-streaming')
    .config('spark.sql.shuffle.partitions', 30)
    .config('spark.sql.streaming.schemaInference', True)
    .config('spark.sql.debug.maxToStringFields', 1000)
    .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', True)
    .getOrCreate()
)

spark.sparkContext.setLogLevel('ERROR')

cnt_columns = [F.col(cols.SHORT_STAY_CNT),
               F.col(cols.STANDARD_STAY_CNT),
               F.col(cols.EXTENDED_STAY_CNT),
               F.col(cols.LONG_STAY_CNT),
               F.col(cols.ERR_STAY_CNT)]

weather_json = (
    spark.read
    .format('kafka')
    .option('kafka.bootstrap.servers', source_params.KAFKA_SERVERS)
    .option('subscribe', source_params.KAFKA_TOPIC_WEATHER)
    .option('startingOffsets', 'earliest')
    .option('endingOffsets', 'latest')
    .load()
    .select(F.col('value').cast('string'))
)

weather = (
    spark.read
    .json(weather_json.rdd.map(lambda r: r.value))
    .withColumn(cols.TEMPERATURE, F.col(cols.TEMPERATURE_JSON).cast('double'))
    .withColumn(cols.WEATHER_DATE, F.to_date(cols.WEATHER_DATE_JSON, 'yyyy-MM-dd'))
    .withColumn(cols.WEATHER_YEAR, F.year(F.col(cols.WEATHER_DATE)))
    .withColumn(cols.WEATHER_WEEK, F.weekofyear(F.col(cols.WEATHER_DATE)))
    .select(
        F.col(cols.ID).alias(cols.HOTEL_ID),
        F.col(cols.TEMPERATURE),
        F.col(cols.WEATHER_DATE),
        F.col(cols.WEATHER_YEAR),
        F.col(cols.WEATHER_WEEK))
    .groupBy(
        F.col(cols.HOTEL_ID),
        F.col(cols.WEATHER_YEAR),
        F.col(cols.WEATHER_WEEK))
    .agg(
        F.avg(F.col(cols.TEMPERATURE))
        .cast('decimal(11,1)')
        .alias(cols.TEMPERATURE))
    .persist()
)

max_week_in_2017 = weather.where(F.col(cols.WEATHER_YEAR) == 2017).agg(F.max(cols.WEATHER_WEEK)).collect()[0][0]

weather_additional_2017 = (
    weather
    .where(
        (F.col(cols.WEATHER_YEAR) == 2016) &
        (F.col(cols.WEATHER_WEEK) > max_week_in_2017))
    .withColumn(cols.WEATHER_YEAR, F.lit(2017))
)

weather = weather.union(weather_additional_2017).persist()

"""
weather.show(20)

weather\
    .select(F.col(cols.WEATHER_YEAR), F.col(cols.WEATHER_WEEK))\
    .distinct()\
    .sort(F.col(cols.WEATHER_YEAR), F.col(cols.WEATHER_WEEK))\
    .show(100)

print(max_week_in_2017)
"""


@F.udf()
def find_most_popular_stay_type(columns, max_val):
    stay_types = ['short', 'standard', 'extended', 'long', 'err']
    return stay_types[columns.index(max_val)]


expedia_row = data_processor.read_expedia_data(spark, 2016)

expedia = (
    data_processor
    .prepare_columns(expedia_row)
    .groupBy(F.col(cols.HOTEL_ID), F.col(cols.CHECK_IN_YEAR), F.col(cols.CHECK_IN_WEEK))
    .agg(F.sum(F.col(cols.SHORT_STAY_CNT)).alias(cols.SHORT_STAY_CNT),
         F.sum(F.col(cols.STANDARD_STAY_CNT)).alias(cols.STANDARD_STAY_CNT),
         F.sum(F.col(cols.EXTENDED_STAY_CNT)).alias(cols.EXTENDED_STAY_CNT),
         F.sum(F.col(cols.LONG_STAY_CNT)).alias(cols.LONG_STAY_CNT),
         F.sum(F.col(cols.ERR_STAY_CNT)).alias(cols.ERR_STAY_CNT))
    .withColumn(cols.MOST_POPULAR_STAY_TYPE,
                find_most_popular_stay_type(F.array(cnt_columns), F.greatest(*cnt_columns)))
    .join(weather, [cols.HOTEL_ID, cols.WEATHER_YEAR, cols.WEATHER_WEEK], 'left_outer')
    .filter(F.col(cols.TEMPERATURE) > 0)
)

expedia_static = (
    expedia
    .withColumnRenamed(cols.SHORT_STAY_CNT, cols.SHORT_STAY_CNT_STATIC)
    .withColumnRenamed(cols.STANDARD_STAY_CNT, cols.STANDARD_STAY_CNT_STATIC)
    .withColumnRenamed(cols.EXTENDED_STAY_CNT, cols.EXTENDED_STAY_CNT_STATIC)
    .withColumnRenamed(cols.LONG_STAY_CNT, cols.LONG_STAY_CNT_STATIC)
    .withColumnRenamed(cols.ERR_STAY_CNT, cols.ERR_STAY_CNT_STATIC)
    .withColumnRenamed(cols.MOST_POPULAR_STAY_TYPE, cols.MOST_POPULAR_STAY_TYPE_STATIC)
    .drop(cols.TEMPERATURE, cols.CHECK_IN_YEAR)
    .persist()
)

print('STREAMING STARTED')

expedia_stream_row = data_processor.read_expedia_stream(spark, 2017)

expedia_stream = (
    data_processor
    .prepare_columns(expedia_stream_row)
    .withWatermark(cols.TIMESTAMP, '0 seconds')
    .groupBy(F.window(F.col(cols.TIMESTAMP), '1 days', '1 days'),
             F.col(cols.HOTEL_ID), F.col(cols.CHECK_IN_YEAR), F.col(cols.CHECK_IN_WEEK))
    .agg(F.sum(F.col(cols.SHORT_STAY_CNT)).alias(cols.SHORT_STAY_CNT),
         F.sum(F.col(cols.STANDARD_STAY_CNT)).alias(cols.STANDARD_STAY_CNT),
         F.sum(F.col(cols.EXTENDED_STAY_CNT)).alias(cols.EXTENDED_STAY_CNT),
         F.sum(F.col(cols.LONG_STAY_CNT)).alias(cols.LONG_STAY_CNT),
         F.sum(F.col(cols.ERR_STAY_CNT)).alias(cols.ERR_STAY_CNT))
    .drop('window')
    .withColumn(cols.MOST_POPULAR_STAY_TYPE,
                find_most_popular_stay_type(F.array(cnt_columns), F.greatest(*cnt_columns)))
    .join(weather, [cols.HOTEL_ID, cols.WEATHER_YEAR, cols.WEATHER_WEEK], 'left_outer')
    .filter(F.col(cols.TEMPERATURE) > 0)
    .join(expedia_static.hint('broadcast'), [cols.HOTEL_ID, cols.WEATHER_WEEK], 'left_outer')
    .withColumn(cols.SHORT_STAY_CNT_DIFF, F.col(cols.SHORT_STAY_CNT) - F.col(cols.SHORT_STAY_CNT_STATIC))
    .withColumn(cols.STANDARD_STAY_CNT_DIFF, F.col(cols.STANDARD_STAY_CNT) - F.col(cols.STANDARD_STAY_CNT_STATIC))
    .withColumn(cols.EXTENDED_STAY_CNT_DIFF, F.col(cols.EXTENDED_STAY_CNT) - F.col(cols.EXTENDED_STAY_CNT_STATIC))
    .withColumn(cols.LONG_STAY_CNT_DIFF, F.col(cols.LONG_STAY_CNT) - F.col(cols.LONG_STAY_CNT_STATIC))
    .withColumn(cols.ERR_STAY_CNT_DIFF, F.col(cols.ERR_STAY_CNT) - F.col(cols.ERR_STAY_CNT_STATIC))
    .withColumn(cols.MOST_POPULAR_STAY_TYPE_IS_EQUAL,
                F.col(cols.MOST_POPULAR_STAY_TYPE) == F.col(cols.MOST_POPULAR_STAY_TYPE_STATIC))
    .drop(cols.SHORT_STAY_CNT_STATIC,
          cols.STANDARD_STAY_CNT_STATIC,
          cols.EXTENDED_STAY_CNT_STATIC,
          cols.LONG_STAY_CNT_STATIC,
          cols.ERR_STAY_CNT_STATIC,
          cols.MOST_POPULAR_STAY_TYPE_STATIC)
    .repartition(1)
)

data_processor.write_output_stream_hdfs(expedia_stream)
