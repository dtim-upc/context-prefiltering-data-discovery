package others

import core.SparkWrapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

object RepartitionDF extends SparkWrapper{



  def readAndWriteParquet( path: String, outputPath: String, numPartitions: Int): Unit = {
    // Read the Parquet file
    val df = spark.read.parquet(path)

    // Repartition the DataFrame into n partitions
    val dfRepartitioned = df.repartition(numPartitions)

    // Write the repartitioned DataFrame back as a Parquet file
    dfRepartitioned.write.mode(SaveMode.Overwrite).parquet(outputPath)

    // Get Hadoop filesystem
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Define paths
    val originalPath = new Path(path)
    val tempPath = new Path(outputPath)

    // Delete original directory if it exists
    if (fs.exists(originalPath)) {
      fs.delete(originalPath, true)
    }

    // Rename (move) the output directory to the original location
    fs.rename(tempPath, originalPath)
  }


  def main(args: Array[String]): Unit = {

//    val basePath = "/Users/javierflores/Koofr/PhD/code/result/data"
//    val tabularPath = s"${basePath}/tabular"
//    val tmpPath = s"${basePath}/tmp"
//
//    val fileNameNoExt = "jikan_charactersv2_1"
//
//    readAndWriteParquet(s"${tabularPath}/${fileNameNoExt}", s"${tmpPath}/${fileNameNoExt}", 16)


    val path = "/Users/javierflores/Koofr/PhD/code/result/data/profiles_norm"
    val output = "/Users/javierflores/Koofr/PhD/code/result/data/profiles_normByDS"
    val df = spark.read.parquet(path)

    df.printSchema()

    println(df.count())
    df.show()

    import spark.implicits._
    // get distinct dataset values
    val datasets = df.select("dataset").distinct().as[String].collect()

    datasets.foreach { dataset =>
      println(dataset)
      val dfFiltered = df.filter(col("dataset") === dataset).drop("original_values", "set_values")
      println(s"writing in ${output}/${dataset}")
      dfFiltered.write.mode(SaveMode.Overwrite).parquet(s"${output}/${dataset}")
    }

//
//    df.printSchema()
////
//    val dsColumn = "dataset"
//    df.write.mode(SaveMode.Overwrite).partitionBy(dsColumn)
//      .parquet("/Users/javierflores/Koofr/PhD/code/result/data/profiles_norm_partitionby")








  }

}
