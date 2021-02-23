package samples

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("spark-batch-test")
      .master("local[*]")
      .getOrCreate()
  }

}
