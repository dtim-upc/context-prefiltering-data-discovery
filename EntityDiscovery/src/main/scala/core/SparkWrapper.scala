package core

import org.apache.spark.sql.SparkSession

trait SparkWrapper {

  val spark = SparkSession.builder().master("local[*]")
//    .config("spark.driver.memory", "5g")
    .config("spark.executor.memory", "9g")
//    .config("spark.driver.maxResultSize", "9g")
    .config("spark.network.timeout","360s")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
  spark.sparkContext.setLogLevel("ERROR")

}
