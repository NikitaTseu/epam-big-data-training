package com.epam.bigdata201.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object App {
  /*
  case class Parameters(
  KafkaServers = "localhost:9092",
  KafkaTopic = "hotels-with-weather",
  PathToExpedia = "hdfs://localhost:9000/datasets/expedia/*.avro") */*/

  val KafkaServers = "localhost:9092"
  val KafkaTopic = "hotels-with-weather"
  val PathToExpedia = "hdfs://localhost:9000/datasets/expedia/*.avro"

  val ColNameId = "id"
  val ColNameName = "name"
  val ColNameCountry = "country"
  val ColNameCity = "city"
  val ColNameHotelId = "hotel_id"
  val ColNameCheckIn = "srch_ci"
  val ColNameIdlePeriod = "idle_period"
  val ColNameVisitIsValid = "is_valid"

  val DatePattern = "yyyy-MM-dd"

  /*class MySparkApp(spark: SparkSession) {
    def TaskOne(): Unit = {
      spark.sql()
    }
  }*/

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder
      .appName("spark-batch")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val hotels_json = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaServers)
      .option("subscribe", KafkaTopic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .select($"value".cast(StringType))

    val hotels = spark.read.json(hotels_json.as[String])
      .dropDuplicates(ColNameId)
      .select(ColNameId, ColNameName, ColNameCountry, ColNameCity)

    val visitsRaw = spark.read
      .format("avro")
      .load(PathToExpedia)

    val windowSpec = Window
      .partitionBy(ColNameHotelId)
      .orderBy(col(ColNameCheckIn).asc)

    val calculateIdleDays = datediff(col(ColNameCheckIn), lag(ColNameCheckIn, 1).over(windowSpec))

    // visits are grouped by hotel_id
    // invalid visits have idle_days value between 2 and 30 (validCondition = false)
    // if group has at least one invalid visit - all visits for this group are treated as invalid
    // if all visits in the group are valid we will have min(true, true, ... , true) = true
    // if some visits in the group are invalid we will have min(false, true, ... , true) = false
    val validCondition = !col(ColNameIdlePeriod).between(2, 30)
    val markValidVisits = min(validCondition).over(windowSpec)

    val visits = visitsRaw
      .withColumn(ColNameCheckIn, to_date(col(ColNameCheckIn), DatePattern))
      .withColumn(ColNameIdlePeriod, calculateIdleDays)
      .withColumn(ColNameVisitIsValid, markValidVisits)

    val invalidHotelIds = visits
      .filter(!col(ColNameVisitIsValid))
      .dropDuplicates(ColNameHotelId)
      .select(ColNameHotelId)

    val invalidHotels = hotels
      .join(invalidHotelIds, hotels.col(ColNameId) === invalidHotelIds.col(ColNameHotelId), "left_semi")

    val validVisits = visits
      .filter(col(ColNameVisitIsValid))
      .join(hotels, hotels.col(ColNameId) === visits.col(ColNameHotelId))

    val countByCountry = validVisits
      .groupBy(ColNameCountry)
      .count()

    val countByCity = validVisits
      .groupBy(ColNameCity)
      .count()

    invalidHotels.show()
    countByCountry.show(10)
    countByCity.show(10)
  }

}
