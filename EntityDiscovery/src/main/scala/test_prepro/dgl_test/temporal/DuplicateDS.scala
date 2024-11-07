package test_prepro.pipeline_new.temporal

import core.SparkWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

object DuplicateDS extends SparkWrapper{

  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest"
    val dsPath = s"${basePath}/datasets"


    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${basePath}/datasetInfo.csv")
      .filter(col("active") === true)


    dsInfo.select("id", "fileName").collect().foreach {
      case Row(id: Int, fileNameNoExt: String) =>

        val df = spark.read.parquet(s"${dsPath}/${fileNameNoExt}")
        df.sample(0.5).write.mode("overwrite").parquet(s"${dsPath}/${fileNameNoExt}_2")

    }

    }

}
