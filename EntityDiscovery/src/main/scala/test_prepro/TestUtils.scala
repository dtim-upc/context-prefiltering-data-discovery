package test_prepro

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUtils {


  def readDatasetTest(spark: SparkSession, path: String, delim: String, multiline: Boolean, nullVal: String, ignoreTrailing: Boolean): DataFrame = {
    val df = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", delim).option("quote", "\"")
      .option("escape", "\"").option("multiline", multiline)
      .option("nullValue", nullVal)
      .option("ignoreTrailingWhiteSpace", ignoreTrailing)
      .csv(s"$path")
    df
  }

}
