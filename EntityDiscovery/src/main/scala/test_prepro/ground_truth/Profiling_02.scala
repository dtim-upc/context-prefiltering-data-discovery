package test_prepro.pipeline_v3

import core.SparkWrapper
import io.github.haross.nuup.nextiajd.NextiaJD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import preprocessing.utils.Utils.createDirIfNotExists

import java.nio.file.{Files, Paths}

object Profiling_02 extends SparkWrapper{


  def main(args: Array[String]): Unit = {


    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2"
    val dsPath = s"${basePath}/datasets"
    val profilesPath =  s"${basePath}/profiles_new"
    val profilesNumPath =  s"${basePath}/numProfiles_new"
    val profilesBoolPath =  s"${basePath}/booleanProfiles_new"
    val profilesStrNorm = s"${basePath}/profiles_norm"
    val profilesStrNormByDS = s"${basePath}/profiles_normByDS"

    val profilesNumNorm = s"${basePath}/numProfiles_norm"
    val profilesNumNormByDS = s"${basePath}/numProfiles_normByDS"


    val option = 4

    createDirIfNotExists(profilesPath)
    createDirIfNotExists(profilesNumPath)

    val recompute = true
    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${basePath}/datasetInfo.csv")

    var dfs = Seq.empty[DataFrame]
    dsInfo.select("id","fileName").collect().foreach{
      case Row(id: Int, fileNameNoExt: String) =>

        option match {
          case 1 =>

            if( (recompute || !Files.isDirectory(Paths.get(s"${profilesPath}/${fileNameNoExt}"))) ){
              println(s"processing file: ${fileNameNoExt} for str profiles")
              val df = spark.read.parquet(s"${dsPath}/${fileNameNoExt}")

              val profiles = NextiaJD.getSTRProfiles(df, fileNameNoExt)
//              profiles.select("colName").showmakemytrip_com-travel_sample_v1 and listings_detailed_v3
              profiles.write.mode("overwrite").parquet(s"${profilesPath}/${fileNameNoExt}")
            } else {

              println(s"--> excluding file ${fileNameNoExt}")

            }

          case 2 =>


              println(s"reading profile for norm ${fileNameNoExt}")
              val df = spark.read.parquet(s"${profilesPath}/${fileNameNoExt}")
              dfs = dfs :+ df


          case 3 =>
//            numeric profiles
            if ( (recompute || !Files.isDirectory(Paths.get(s"${profilesNumPath}/${fileNameNoExt}")))) {
              println(s"processing file: ${fileNameNoExt} for numeric profiles")
              val df = spark.read.parquet(s"${dsPath}/${fileNameNoExt}")

              try {
                val profiles = NextiaJD.getNumProfiles(df, fileNameNoExt)
                profiles.write.mode("overwrite").parquet(s"${profilesNumPath}/${fileNameNoExt}")
              } catch {
                case e:io.github.haross.nuup.nextiajd.exceptions.NoNumericColumnException =>
                  println(s"dataset ${fileNameNoExt} does not have numeric columns")
              }

            } else {

              println(s"--> excluding file ${fileNameNoExt}")

            }

          case 4 =>
//            normalizing num profiles
            val path = Paths.get(s"${profilesNumPath}/${fileNameNoExt}")
            if (Files.exists(path)) {

              println(s"reading num profile for norm ${fileNameNoExt}")
              val df = spark.read.parquet(s"${profilesNumPath}/${fileNameNoExt}")
              dfs = dfs :+ df
            }

          case 5 =>

            // boolean profiles

            if ((recompute || !Files.isDirectory(Paths.get(s"${profilesBoolPath}/${fileNameNoExt}")))) {
              println(s"processing file: ${fileNameNoExt} for boolean profiles")
              val df = spark.read.parquet(s"${dsPath}/${fileNameNoExt}")

              try {
                val profiles = NextiaJD.getBooleanProfiles(df, fileNameNoExt)
                profiles.write.mode("overwrite").parquet(s"${profilesBoolPath}/${fileNameNoExt}")
              } catch {
                case e: io.github.haross.nuup.nextiajd.exceptions.NoBooleanColumnException =>
                  println(s"--->dataset ${fileNameNoExt} does not have boolean columns")
              }

            } else {

              println(s"--> excluding file ${fileNameNoExt}")

            }

        }




    }

    option match {

      case 2 =>

      println("normalizing str profiles")
      NextiaJD.normalizeStrProfiles(dfs).repartition(24).write.mode("overwrite")
        .parquet(profilesStrNorm)


      val df = spark.read.parquet(profilesStrNorm)
      // save norm by datasets partition
      import spark.implicits._
      val datasets = df.select("dataset").distinct().as[String].collect()

      datasets.foreach { dataset =>
        println(dataset)
        val dfFiltered = df.filter(col("dataset") === dataset).drop("original_values", "set_values")
        println(s"writing in ${profilesStrNormByDS}/${dataset}")
        dfFiltered.write.mode(SaveMode.Overwrite).parquet(s"${profilesStrNormByDS}/${dataset}")
      }

      case 4 =>


        println("normalizing num profiles")
        NextiaJD.normalizeNumProfiles(dfs).repartition(24).write.mode("overwrite")
          .parquet(profilesNumNorm)


        val df = spark.read.parquet(profilesNumNorm)
        // save norm by datasets partition
        import spark.implicits._
        val datasets = df.select("dataset").distinct().as[String].collect()

        datasets.foreach { dataset =>
          println(dataset)
          val dfFiltered = df.filter(col("dataset") === dataset).drop("original_values", "set_values")
          println(s"writing in ${profilesNumNormByDS}/${dataset}")
          dfFiltered.write.mode(SaveMode.Overwrite).parquet(s"${profilesNumNormByDS}/${dataset}")
        }

      case _ =>




    }

  }

}
