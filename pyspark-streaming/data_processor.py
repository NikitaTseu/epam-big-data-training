from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from constants import source_params, cols


def read_expedia_data(spark: SparkSession, year) -> DataFrame:
    return (
        spark.read.format('avro')
        .load(source_params.PATH_TO_EXPEDIA + cols.CHECK_IN_YEAR + '=' + str(year) + '/*.avro')
    )


def prepare_columns(expedia):
    return (expedia
            .select(cols.HOTEL_ID, cols.CHECK_IN, cols.CHECK_OUT)
            .withColumn(cols.CHECK_IN, F.to_date(cols.CHECK_IN, 'yyyy-MM-dd'))
            .withColumn(cols.CHECK_OUT, F.to_date(cols.CHECK_OUT, 'yyyy-MM-dd'))
            .withColumn(cols.CHECK_IN_YEAR, F.year(F.col(cols.CHECK_IN)))
            .withColumn(cols.CHECK_IN_WEEK, F.weekofyear(F.col(cols.CHECK_IN)))
            .withColumn(cols.STAY_DURATION, F.datediff(F.col(cols.CHECK_OUT), F.col(cols.CHECK_IN)))
            .filter(F.col(cols.STAY_DURATION).isNotNull())
            .withColumn(cols.TIMESTAMP, F.to_date(F.col(cols.CHECK_IN_YEAR).cast('string'), 'yyyy'))
            .withColumn(cols.TIMESTAMP,
                        F.expr('to_timestamp(date_add(' + cols.TIMESTAMP
                               + ', ' + cols.CHECK_IN_WEEK + '), \'yyyy-MM-dd\')'))
            .withColumn(cols.SHORT_STAY_CNT,
                        (F.col(cols.STAY_DURATION) == 1).cast('integer'))
            .withColumn(cols.STANDARD_STAY_CNT,
                        ((1 < F.col(cols.STAY_DURATION)) & (F.col(cols.STAY_DURATION) <= 7)).cast('integer'))
            .withColumn(cols.EXTENDED_STAY_CNT,
                        ((7 < F.col(cols.STAY_DURATION)) & (F.col(cols.STAY_DURATION) <= 14)).cast('integer'))
            .withColumn(cols.LONG_STAY_CNT,
                        ((14 < F.col(cols.STAY_DURATION)) & (F.col(cols.STAY_DURATION) <= 28)).cast('integer'))
            .withColumn(cols.ERR_STAY_CNT,
                        (~((1 <= F.col(cols.STAY_DURATION)) & (F.col(cols.STAY_DURATION) <= 28))).cast('integer'))
            )


def read_expedia_stream(spark: SparkSession, year):
    return (
        spark.readStream
        .format('avro')
        .option('maxFilesPerTrigger', 7)
        .option('triggerInterval', 1)
        .load(source_params.PATH_TO_EXPEDIA + cols.CHECK_IN_YEAR + '=' + str(year) + '/')
    )


def write_output_stream_console(output_df):
    (output_df.writeStream
     .format('console')
     .outputMode('append')
     .option('checkpointLocation', source_params.CHECKPOINT_LOCATION)
     .start()
     .awaitTermination())


def write_output_stream_hdfs(output_df):
    (output_df.writeStream
     .format('parquet')
     .outputMode('append')
     .option('header', True)
     .option('checkpointLocation', source_params.CHECKPOINT_LOCATION)
     .option('path', source_params.OUTPUT_PATH)
     .start()
     .awaitTermination())
