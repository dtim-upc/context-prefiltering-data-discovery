package groundTruth

import core.SparkWrapper
import io.github.haross.nuup.bootstrap.Bootstrapping.generateSchemaGraph
import io.github.haross.nuup.bootstrap.DataFrameProcessor.flattenDataFrame
import org.apache.spark.sql.Row
import preprocessing.utils.Utils.{createDirIfNotExists, writeSingleParquet}

import java.io.File
import java.nio.file.{Files, Paths}

object GraphAndTabular extends SparkWrapper {


  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/data"
    val dsPath = s"${basePath}/datasets"
    val graphPath = s"${basePath}/graphs"
    val tabularPath = s"${basePath}/tabular"
    val recompute = false

    createDirIfNotExists(graphPath)
    createDirIfNotExists(tabularPath)
    val fileType = "parquet"

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${basePath}/datasetInfo.csv")

    dsInfo.printSchema()

    dsInfo.select("id","fileName","graph","tabular").collect().foreach{
      case Row(id: Int, fileNameNoExt: String, graphF:Boolean, tabularF:Boolean) =>

          println(s"processing file: ${fileNameNoExt}")
        val df = fileType match {
          case "parquet" =>
            spark.read.parquet(s"${dsPath}/${fileNameNoExt}")
          case "json" =>
            spark.read.option("multiline", true).json(s"${dsPath}/${fileNameNoExt}.json")
        }
        if(graphF && (recompute || !new File(s"$graphPath/$fileNameNoExt.ttl").exists()) ) {

            println("generating graph schema")
            val graph = generateSchemaGraph(df, fileNameNoExt , Option(id.toString))
            graph.write(s"${graphPath}/${fileNameNoExt}.ttl","ttl")

        }

//        if(tabularF && (recompute || !Files.isDirectory(Paths.get(s"$tabularPath/$fileNameNoExt"))) ){
//          println("generating tabular schema")
//          val tabulardf = flattenDataframe(df)
//          tabulardf.write.mode("overwrite").parquet(s"${tabularPath}/${fileNameNoExt}")
//        }


    }


  }

}
