package com.epam.bigdata201.spark

import com.epam.bigdata201.spark.ColNames._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, ColumnName, DataFrame, SaveMode, SparkSession}

class MySparkApp(spark: SparkSession, inputParameters: InputParameters) {
  import spark.implicits._

  val DatePattern = "yyyy-MM-dd"

  private val idleDaysWindow = Window
    .partitionBy(ColNameHotelId)
    .orderBy(col(ColNameCheckIn).asc)

  private val validationWindow = Window
    .partitionBy(ColNameHotelId)

  /*  Visits are grouped by hotel_id and ordered by check-in date.
      For every visit "idle_period" is a difference in days between
      its check-in date and check-in date of the previous visit (null for first visit in group) */
  private val calculateIdleDays = datediff(col(ColNameCheckIn), lag(ColNameCheckIn, 1).over(idleDaysWindow))

  /*  Visits are grouped by hotel_id.
      Invalid visits have "idle_period" value between 2 and 30 (validCondition = false).
      If group has at least one invalid visit - all visits for this group are treated as invalid.
      If all visits in the group are valid we will have min(true, true, ... , true) = true.
      If some visits in the group are invalid we will have min(false, true, ... , true) = false. */
  private val validCondition = !col(ColNameIdlePeriod).between(2, 30)
  private val markValidVisits = min(validCondition).over(validationWindow)

  def readHotelsData(): DataFrame = {
    val hotels_json = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", inputParameters.kafkaServers)
      .option("subscribe", inputParameters.kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .select($"value".cast(StringType))

    spark.read.json(hotels_json.as[String])
      .dropDuplicates(ColNameId)
      .select(ColNameId, ColNameName, ColNameCountry, ColNameCity)
  }

  def readVisitsData(): DataFrame = {
    spark.read
      .format("avro")
      .load(inputParameters.pathToExpedia)
  }

  def enrichVisits(visitsRaw: DataFrame): DataFrame = {
    visitsRaw
      .withColumn(ColNameCheckIn, to_date(col(ColNameCheckIn), DatePattern))
      .withColumn(ColNameIdlePeriod, calculateIdleDays)
      .na.fill(-1, Seq(ColNameIdlePeriod))
      .withColumn(ColNameVisitIsValid, markValidVisits)
  }

  def findInvalidHotelIds(visits: DataFrame): DataFrame = {
    visits
      .filter(!col(ColNameVisitIsValid))
      .dropDuplicates(ColNameHotelId)
      .select(ColNameHotelId)
  }

  def findInvalidHotels(hotels: DataFrame, invalidHotelIds: DataFrame): DataFrame = {
    hotels.join(
      invalidHotelIds,
      hotels.col(ColNameId) === invalidHotelIds.col(ColNameHotelId),
      "left_semi")
  }

  def findValidVisits(hotels: DataFrame, visits: DataFrame): DataFrame = {
    visits
      .filter(col(ColNameVisitIsValid))
  }

  def countByField(dataFrame: DataFrame, fieldName: String): DataFrame = {
    dataFrame
      .groupBy(fieldName)
      .count()
  }

  // writes dataframe to HDFS with partitioning by provided column name
  def writeWithPartitioning(dataFrame: DataFrame, colName: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).partitionBy(colName).parquet(inputParameters.outputPath)
  }

}
