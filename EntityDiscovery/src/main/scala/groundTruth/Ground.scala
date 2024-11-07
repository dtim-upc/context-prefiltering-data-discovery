package groundTruth

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import core.SparkWrapper
import groundTruth.GraphAndTabular.spark
import io.github.haross.nuup.nextiajd.NextiaJD
import preprocessing.utils.Utils.{createDirIfNotExists, mergeParquetFiles}

import java.io.File
import java.nio.file.{Files, Paths}

object Ground extends SparkWrapper{

  def readDataset(path: String, alias:String): Dataset[Row] = {


    val dataset = spark.read.parquet(path)
    val datasetN = dataset.withColumn("tmp",
      explode(array(dataset.schema.filter(a => a.dataType.isInstanceOf[StringType])
        .map(a => a.name).map(name => struct(lit(name).as("colName") ,col(name).as("colVal") )): _*)))
      .withColumn("dataset", lit(alias) )
      .select(col("dataset"), col("tmp.colName"), col("tmp.colVal"))
      .withColumn("colName", trim(lower(col("colName"))))
      .withColumn("colVal", trim(lower(col("colVal"))))
      .filter(col("colVal") =!= "") //filter empty strings
      .groupBy(col("dataset"),col("colName"))
      .agg(collect_set(col("colVal")).alias("values")) //collect_set eliminates duplicates
      .withColumn("cardinality", size(col("values")))
    datasetN
  }

  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/data"
    val tabularPath = s"${basePath}/tabular"

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${basePath}/datasetInfo.csv")

    val dfs = Seq.empty[DataFrame]
//    dsInfo.select("id","fileName").where(col("fileName").contains("jikan_charactersv2")).collect().foreach{
//      case Row(id: Int, fileNameNoExt: String) =>
//
//        println(s"processing file: ${fileNameNoExt}")
//        val df = readDataset(s"${tabularPath}/${fileNameNoExt}", fileNameNoExt)
////      df.show()
//      //        df.select("colName","cardinality").show(40,false)
//
//    }

    val df = spark.read.parquet(s"${tabularPath}/jikan_charactersv2_1")
    df.printSchema()
    df.show(false)
    println(df.count())
//    val fields = df.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(_.name).take(4)
//    println(fields.toSeq)

//    val profiles = NextiaJD.generateBaseStringDF(df.drop("about").distinct())
//    profiles.write.mode("overwrite").parquet("/Users/javierflores/Koofr/PhD/code/EntityDiscovery/src/main/resources/tmp")


    //    val dsPaths = "/Users/javierflores/Koofr/PhD/code/result/data/tabular"
//    val basePath = "src/main/resources/groundTruth"
//    val outputTmpPath = s"${basePath}/tmp"
//    val outputPath = s"${basePath}/out"
//
//    createDirIfNotExists(basePath)
//    createDirIfNotExists(outputTmpPath)
//    createDirIfNotExists(outputPath)


//    import spark.implicits._
//
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//
//    val all = os.list(os.Path(dsPaths))
//      .map(p => readDataset(p.toString()))
//      .reduce(_.unionAll(_))
//
//    val A = all.withColumnRenamed("dataset","datasetA")
//      .withColumnRenamed("colName", "attA")
//      .withColumnRenamed("values","valuesA")
//      .withColumnRenamed("cardinality","cardinalityA")
//
//    val B = all.withColumnRenamed("dataset","datasetB")
//      .withColumnRenamed("colName", "attB")
//      .withColumnRenamed("values","valuesB")
//      .withColumnRenamed("cardinality","cardinalityB")
//
//    val cross = A.crossJoin(B)
//      .filter($"datasetA" =!= $"datasetB") //avoid self-joins
//      .withColumn("C", size(array_intersect($"valuesA",$"valuesB"))/$"cardinalityA")
//      .filter($"C" > 0)
//      .withColumn("K", least($"cardinalityA",$"cardinalityB")/greatest($"cardinalityA",$"cardinalityB"))
//      .select($"datasetA",$"attA",$"datasetB",$"attB",$"C",$"K")
//
////      .coalesce(1)
//    cross
//      .write
////      .option("header","true")
////      .option("sep",",")
//      .mode("overwrite")
//      .parquet(outputTmpPath)
//
//    mergeParquetFiles(spark, outputTmpPath,outputPath)

  }

}
