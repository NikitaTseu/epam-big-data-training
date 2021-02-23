package samples

import org.scalatest.FunSpec
import com.epam.bigdata201.spark.ColNames._
import com.epam.bigdata201.spark.{InputParameters, MySparkApp}
import org.apache.log4j.{Level, Logger}

class MySparkAppSpec extends FunSpec with SparkSessionTestWrapper {
  Logger.getLogger("org").setLevel(Level.WARN)
  import spark.implicits._

  val sparkApp = new MySparkApp(spark, InputParameters())

  describe(".enrichVisits") {

    it("should calculate idle days and recognize valid and invalid visits") {
      val testVisitsRaw = Seq(
        ("1", "2020-02-10"), // valid
        ("1", "2020-02-11"), // valid
        ("1", "2020-02-12"), // valid
        ("2", "2020-02-10"), // invalid
        ("2", "2020-02-15"), // invalid
        ("2", "2020-02-16"), // invalid
        ("2", "2020-02-17")) // invalid
        .toDF(ColNameHotelId, ColNameCheckIn)

      val testVisits = testVisitsRaw
        .transform(sparkApp.enrichVisits)
        .orderBy(ColNameHotelId, ColNameCheckIn)

      val expectedIdleDays = Array(-1, 1, 1, -1, 5, 1, 1)
      val actualIdleDays = testVisits.select(ColNameIdlePeriod).collect().map(_.getInt(0))

      val expectedValidFlags = Array(true, true, true, false, false, false, false)
      val actualValidFlags = testVisits.select(ColNameVisitIsValid).collect().map(_.getBoolean(0))

      assert(expectedIdleDays.deep === actualIdleDays.deep)
      assert(expectedValidFlags.deep === actualValidFlags.deep)
    }

  }
}
