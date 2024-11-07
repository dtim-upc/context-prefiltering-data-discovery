package test_prepro.pipeline_v3

import core.SparkWrapper
import io.github.haross.nuup.bootstrap.Bootstrapping.generateSchemaGraph
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import preprocessing.utils.Utils.createDirIfNotExists

import java.io.File

object GraphSchemas_01 extends SparkWrapper {


  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2"
    val dsPath = s"${basePath}/datasets"
    val graphPath = s"${basePath}/graphs"
    val recompute = true

    createDirIfNotExists(graphPath)

    val fileType = "parquet"

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${basePath}/datasetInfo.csv")
      .filter(col("active") === true)


    dsInfo.select("id","fileName").collect().foreach{
      case Row(id: Int, fileNameNoExt: String) =>

          println(s"processing file: ${fileNameNoExt}")
        val df = fileType match {
          case "parquet" =>
            spark.read.parquet(s"${dsPath}/${fileNameNoExt}")
          case "json" =>
            spark.read.option("multiline", true).json(s"${dsPath}/${fileNameNoExt}.json")
        }
        if(recompute || !new File(s"$graphPath/$fileNameNoExt.ttl").exists())  {

            println("generating graph schema")
            val graph = generateSchemaGraph(df, fileNameNoExt , Option(id.toString))

//          val q = graph.runAQuery("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\nSELECT ?resource (COUNT(?label) AS ?labelCount)\nWHERE {\n    ?resource rdfs:label ?label .\n}\nGROUP BY ?resource\nHAVING (COUNT(?label) > 1)")
//          if(q.hasNext){
//            val n = q.next()
//            println(s"${fileNameNoExt} ERROR ${n.get("resource")} ${n.get("labelCount")}")
//          }

            graph.write(s"${graphPath}/${fileNameNoExt}.ttl","ttl")

        }

    }


  }

}
