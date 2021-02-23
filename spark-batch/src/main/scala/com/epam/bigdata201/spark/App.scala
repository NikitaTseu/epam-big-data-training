package com.epam.bigdata201.spark

import com.epam.bigdata201.spark.ColNames._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, year}

object App {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder
      .appName("spark-batch")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    println("CURRENT CLUSTER MANAGER IS ---> " + spark.sparkContext.getConf.get("spark.master", "DEFAULT"))

    val sparkApp = new MySparkApp(spark, InputParameters())

    val hotels = sparkApp.readHotelsData()
    val visits = sparkApp.readVisitsData().transform(sparkApp.enrichVisits)
    val validVisits = sparkApp.findValidVisits(hotels, visits)

    val invalidHotelIds = sparkApp.findInvalidHotelIds(visits)
    val invalidHotels = sparkApp.findInvalidHotels(hotels, invalidHotelIds)

    val countByCountry = sparkApp.countByField(
      validVisits.join(hotels, hotels.col(ColNameId) === visits.col(ColNameHotelId)), ColNameCountry)
    val countByCity = sparkApp.countByField(
      validVisits.join(hotels, hotels.col(ColNameId) === visits.col(ColNameHotelId)), ColNameCity)

    invalidHotels.show()
    countByCountry.show(10)
    countByCity.show(10)

    sparkApp.writeWithPartitioning(
      validVisits.withColumn(ColNameCheckInYear, year(col(ColNameCheckIn))), ColNameCheckInYear)
  }

}
