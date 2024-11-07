package test_prepro.pipeline_new

import core.SparkWrapper
import groundTruth.CardinalityAndGraphs.{GraphAndFilename, loadSchemaGraphs_parallel}
import io.github.haross.nuup.bootstrap.DataFrameProcessor.splitByEntity
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import test_prepro.pipeline.GroundWithEntities.getPropertiesWithEntities

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Paths, SimpleFileVisitor}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.parallel.ParSeq

object GroundTest_new extends SparkWrapper{

  import spark.implicits._

  case class AttComparison( dsA: String,  attA: String,  cardinalityA: Double,  dsB: String, attB: String,
                            cardinalityB: Double,  joinSize: Double,  containmentAB: Double,  containmentBA: Double)


  def generateBaseDFs(dataset: DataFrame, alias:String): Dataset[Row] = {

    val maxLength = 300

    val datasetN = dataset.withColumn("tmp",
      explode(array(dataset.schema.filter(a => a.dataType.isInstanceOf[StringType])
        .map(a => a.name).map(name => struct(lit(name).as("colName") ,col(name).as("colVal") )): _*)))
//        .map(a => a.name).map(name => {
//        val truncatedCol = substring(col(name).cast(StringType), 1, maxLength)
//        struct(lit(name).as("colName") ,truncatedCol.as("colVal") )
//      }): _*)))
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

  def loadDatasets(dsInfo: DataFrame, dsPath: String): DataFrame = {
    println(s"number of ds ${dsInfo.count()}")
    var allDFs = Seq.empty[DataFrame]
    dsInfo.select("id", "fileName").collect().foreach {
      case Row(id: Int, fileNameNoExt: String) =>

        println(s"processing file: ${fileNameNoExt}")
        val df = spark.read.parquet(s"${dsPath}/${fileNameNoExt}")

        val dfs = splitByEntity(df).filter(df => df.schema.fields.exists(_.dataType == StringType))

        //        println("dfs: ", dfs.size)
        allDFs = allDFs :+ dfs.map(x => generateBaseDFs(x, fileNameNoExt)).reduce(_ union _)

    }

    val all = allDFs.reduce(_.unionAll(_))
    all.show()
    all
  }

  def readDatasets(info: ParSeq[(Int, String)], dsPath: String): Map[String, DataFrame] = {

    // mapDS contains as key the filename and as value the respective dataframe
    info.foldLeft(Map.empty[String, DataFrame]) { case (mapDS, (i, fileNameNoExt)) =>
//      println(s"readDatasets reading $fileNameNoExt")
      val df = spark.read.parquet(s"$dsPath/$fileNameNoExt")
      val dfs = splitByEntity(df).filter(df => df.schema.fields.exists(_.dataType == StringType))
      val basedf = dfs.map(x => generateBaseDFs(x, fileNameNoExt)).reduce(_ union _)

      mapDS + (fileNameNoExt -> basedf)
    }

  }


  def setEntities(ground:DataFrame, sourceGraphs:  Map[Int,GraphAndFilename]): DataFrame = {

    val proAndEntities = getPropertiesWithEntities(sourceGraphs)
    val propertiesInfoDf = proAndEntities.toDF.withColumn("alias", lower(col("alias")))


    // join on attA and attB
    val dfWithParentA = ground
      .join(propertiesInfoDf.withColumnRenamed("aliasParent", "aliasParentA")
        .withColumnRenamed("parentIRI", "parentIRIA"),
        ground("attA") === propertiesInfoDf("alias") && ground("datasetA") === propertiesInfoDf("filename"), "left")
      .drop("alias")
      .drop("filename")

    // join on attB and datasetB
    val dfWithParentsAB = dfWithParentA
      .join(propertiesInfoDf.withColumnRenamed("aliasParent", "aliasParentB")
        .withColumnRenamed("parentIRI", "parentIRIB"),
        dfWithParentA("attB") === propertiesInfoDf("alias") && dfWithParentA("datasetB") === propertiesInfoDf("filename"), "left")
      .drop("alias")
      .drop("filename")
//    dfWithParentsAB.show(false)


//    println("Size ground", ground.count())
//    println("size new ground", dfWithParentsAB.count)

    dfWithParentsAB

  }

  def deleteFolderWithContent(path: String): Unit = {
    val directory = Paths.get(path)

    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor[java.nio.file.Path]() {
        override def visitFile(file: java.nio.file.Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: java.nio.file.Path, exc: java.io.IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
  }


  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2"
    val dsPath = s"${basePath}/datasets"
    val outputPath = s"${basePath}/ground_prueba"
    val graphsWithMetadataPath = s"${basePath}/graphsWithMetadata"

    val option = 3

    val recompute = false


    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").csv(s"${basePath}/datasetInfo.csv")
      .filter(col("active") === true)
//      .limit(1)

    val parIdAndFileNames = dsInfo.select("id", "fileName").as[(Int, String)].collect().toList.par
    val mapDS = readDatasets(parIdAndFileNames, dsPath)
    println(s"number read ds: ${mapDS.size}")

//    val keys = mapDS.keySet
//
//    for (k <- keys) {
//      println(s"reading ${k}")
//      mapDS(k).show(5)
//    }

    val sourceGraphs = loadSchemaGraphs_parallel(parIdAndFileNames, graphsWithMetadataPath)

    option match {

      case -4 =>
        val comb = mapDS.keySet.toSeq.combinations(2).map(pair => if (pair(0) < pair(1)) (pair(0), pair(1)) else (pair(1), pair(0))).toList
        val ground = comb.map { case (fileA, fileB) =>

          if( ( !Files.isDirectory(Paths.get(s"${outputPath}/${fileA}_${fileB}"))) ){
            println(s"-------> *** dataset ${fileA}_${fileB} contains no rows!!!! ***")

            val df1 = spark.read.parquet(s"$dsPath/${fileA}")
            val dfs1 = splitByEntity(df1).filter(df => df.schema.fields.exists(_.dataType == StringType))
            val basedf1 = dfs1.map(x => generateBaseDFs(x, fileA)).reduce(_ union _)

            basedf1.write.mode("overwrite").parquet(s"${outputPath}/hola1")
            val basedf11 = spark.read.parquet(s"${outputPath}/hola1")

            val df2 = spark.read.parquet(s"$dsPath/${fileB}")
            val dfs2 = splitByEntity(df2).filter(df => df.schema.fields.exists(_.dataType == StringType))
            val basedf2 = dfs2.map(x => generateBaseDFs(x, fileB)).reduce(_ union _)

            basedf2.write.mode("overwrite").parquet(s"${outputPath}/hola2")
            val basedf22 = spark.read.parquet(s"${outputPath}/hola2")

            val dsA = basedf11
              .withColumnRenamed("dataset", "datasetA")
              .withColumnRenamed("colName", "attA")
              .withColumnRenamed("values", "valuesA")
              .withColumnRenamed("cardinality", "cardinalityA")
            val dsB = basedf22
              .withColumnRenamed("dataset", "datasetB")
              .withColumnRenamed("colName", "attB")
              .withColumnRenamed("values", "valuesB")
              .withColumnRenamed("cardinality", "cardinalityB")


            val cross = dsA.alias("A").join(dsB.alias("B"), $"A.datasetA" < $"B.datasetB")
              .withColumn("sizeJoin", size(array_intersect($"valuesA", $"valuesB")))
              .withColumn("containmentA", $"sizeJoin" / $"cardinalityA")
              .withColumn("containmentB", $"sizeJoin" / $"cardinalityB")
              .select($"datasetA", $"attA", $"cardinalityA", $"datasetB", $"attB", $"cardinalityB", $"sizeJoin", $"containmentA", $"containmentB")


            cross.write.mode("overwrite").parquet(s"${outputPath}/${fileA}_${fileB}")

            val dfG = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
            println(s"now has ${dfG.count()}")
            dfG
          } else {
            var dfG = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
            val countRows = dfG.count()
            if (countRows == 0) {
              println(s"-------> *** dataset ${fileA}_${fileB} contains no rows!!!! ***")

              val df1 = spark.read.parquet(s"$dsPath/${fileA}")
              val dfs1 = splitByEntity(df1).filter(df => df.schema.fields.exists(_.dataType == StringType))
              val basedf1 = dfs1.map(x => generateBaseDFs(x, fileA)).reduce(_ union _)

              basedf1.write.mode("overwrite").parquet(s"${outputPath}/hola1")
              val basedf11 = spark.read.parquet(s"${outputPath}/hola1")

              val df2 = spark.read.parquet(s"$dsPath/${fileB}")
              val dfs2 = splitByEntity(df2).filter(df => df.schema.fields.exists(_.dataType == StringType))
              val basedf2 = dfs2.map(x => generateBaseDFs(x, fileB)).reduce(_ union _)

              basedf2.write.mode("overwrite").parquet(s"${outputPath}/hola2")
              val basedf22 = spark.read.parquet(s"${outputPath}/hola2")

              val dsA = basedf11
                .withColumnRenamed("dataset", "datasetA")
                .withColumnRenamed("colName", "attA")
                .withColumnRenamed("values", "valuesA")
                .withColumnRenamed("cardinality", "cardinalityA")
              val dsB = basedf22
                .withColumnRenamed("dataset", "datasetB")
                .withColumnRenamed("colName", "attB")
                .withColumnRenamed("values", "valuesB")
                .withColumnRenamed("cardinality", "cardinalityB")


              val cross = dsA.alias("A").join(dsB.alias("B"), $"A.datasetA" < $"B.datasetB")
                .withColumn("sizeJoin", size(array_intersect($"valuesA", $"valuesB")))
                .withColumn("containmentA", $"sizeJoin" / $"cardinalityA")
                .withColumn("containmentB", $"sizeJoin" / $"cardinalityB")
                .select($"datasetA", $"attA", $"cardinalityA", $"datasetB", $"attB", $"cardinalityB", $"sizeJoin", $"containmentA", $"containmentB")


              cross.write.mode("overwrite").parquet(s"${outputPath}/${fileA}_${fileB}")

              dfG = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
              println(s"now has ${dfG.count()}")

            }
            dfG

          }
        }.seq.reduce(_ union _)

      case -3 =>

        val file1 = "0401230100_BCN_Nomenclator_v1"
        val file2 = "0401230100_BCN_Nomenclator_v3"


        println(s"files: ${file1} and ${file2}")

        val df1 = spark.read.parquet(s"$dsPath/${file1}")
        val dfs1 = splitByEntity(df1).filter(df => df.schema.fields.exists(_.dataType == StringType))
        val basedf1 = dfs1.map(x => generateBaseDFs(x, file1)).reduce(_ union _)

        basedf1.write.mode("overwrite").parquet(s"${outputPath}/hola1")
        val basedf11 = spark.read.parquet(s"${outputPath}/hola1")
        basedf11.select("dataset").show(false)
        basedf11.printSchema()

        val df2 = spark.read.parquet(s"$dsPath/${file2}")
        val dfs2 = splitByEntity(df2).filter(df => df.schema.fields.exists(_.dataType == StringType))
        val basedf2 = dfs2.map(x => generateBaseDFs(x, file2)).reduce(_ union _)

        basedf2.write.mode("overwrite").parquet(s"${outputPath}/hola2")
        val basedf22 = spark.read.parquet(s"${outputPath}/hola2")
        basedf22.select("dataset").show(false)

        val dsA = basedf11
          .withColumnRenamed("dataset", "datasetA")
          .withColumnRenamed("colName", "attA")
          .withColumnRenamed("values", "valuesA")
          .withColumnRenamed("cardinality", "cardinalityA")
        val dsB = basedf22
          .withColumnRenamed("dataset", "datasetB")
          .withColumnRenamed("colName", "attB")
          .withColumnRenamed("values", "valuesB")
          .withColumnRenamed("cardinality", "cardinalityB")


        val cross = dsA.alias("A").join(dsB.alias("B"), $"A.datasetA" < $"B.datasetB")
          .withColumn("sizeJoin", size(array_intersect($"valuesA", $"valuesB")))
          .withColumn("containmentA", $"sizeJoin" / $"cardinalityA")
          .withColumn("containmentB", $"sizeJoin" / $"cardinalityB")
          .select($"datasetA", $"attA", $"cardinalityA", $"datasetB", $"attB", $"cardinalityB", $"sizeJoin", $"containmentA", $"containmentB")


        cross.write.mode("overwrite").parquet(s"${outputPath}/${file1}_${file2}")
        val dfG = spark.read.parquet(s"${outputPath}/${file1}_${file2}")
        dfG.orderBy($"sizeJoin".desc).show(25)
        println(s"${dfG.count}")


      case -2 =>
        val list = (1 to 129).map(i => s"element$i").toSeq.combinations(2).map(pair => if (pair(0) < pair(1)) (pair(0), pair(1)) else (pair(1), pair(0))).toList

        println(s"list size ${list.size}")


      case -1 =>

        val da = spark.read.parquet(s"${outputPath}/aac_intakes_v3_aac_intakes_v1")
        da.show

//        mapDS("aac_intakes_v3").show()

        mapDS("aac_intakes_v1").show()

      case 0 =>

        val comb = mapDS.keySet.toSeq.sorted.combinations(2).map(pair => if (pair(0) < pair(1)) (pair(0), pair(1)) else (pair(1), pair(0))).toList
        val numCombinations = comb.size
        val counter = new AtomicInteger(0)

        // 1. Read and merge parquets in groups of 500
        val groupSize = 500
        val tempPaths = comb.grouped(groupSize).zipWithIndex.flatMap { case (group, index) =>
          println(s"Merging group ${index + 1} of ${comb.size / groupSize}")

          val mergedDf = group.par.map { case (fileA, fileB) =>
            spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
          }.seq.reduce(_ union _)

          val tempPath = s"${outputPath}/temp_merged_${index}.parquet"
          mergedDf.write.parquet(tempPath)

          Some(tempPath)
        }.toList

        // 2. Merge the resulting smaller parquets into one final parquet
        println("Merging all intermediate parquets into a final parquet...")
        val finalDf = tempPaths.map(path => spark.read.parquet(path)).reduce(_ union _)
        finalDf.write.parquet(s"${outputPath}/final_merged.parquet")


        //        println("merging files....")
//        val ground_tmp = comb.map { case (fileA, fileB) =>
//          val count = counter.incrementAndGet()
//          println(s"${count} out of ${numCombinations}: reading ground truth for ${fileA} and ${fileB}")
//          val dfG = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
//          val countRows = dfG.count()
//          if (countRows == 0) {
//            println(s"-------> *** dataset ${fileA}_${fileB} contains no rows!!!! ***")
//            //            dfG.show(2)
////            deleteFolderWithContent(s"${outputPath}/${fileA}_${fileB}")
//          }
//          dfG
//        }.seq.reduce(_ union _)

//        ground_tmp.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath + "csv_tmp")
        val ground = spark.read.parquet(s"${outputPath}/final_merged.parquet")
        //        ground.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath+"csv")

        println("adding entities....")
        val groundWithEntities = setEntities(ground, sourceGraphs)
        groundWithEntities.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath + "csv")


      case 1 =>
        println("--------- computing ground concurrent --------")
//        val isIncremental = false

        // make sure that for every pair, the first value is always less than the second value (and swap if necessary)
        // since the join condition require it
        val comb =  mapDS.keySet.toSeq.sorted.combinations(2).map(pair => if (pair(0) < pair(1)) (pair(0), pair(1)) else (pair(1), pair(0))).toList.par
        val numCombinations = comb.size
        val counter = new AtomicInteger(0)

        comb.foreach{ case (fileA, fileB) =>

          val count = counter.incrementAndGet()

          if( (recompute || !Files.isDirectory(Paths.get(s"${outputPath}/${fileA}_${fileB}"))) ){
            println(s"${count} out of ${numCombinations}: Computing ground truth for ${fileA} and ${fileB}")
            val dsA = mapDS(fileA)
              .withColumnRenamed("dataset", "datasetA")
              .withColumnRenamed("colName", "attA")
              .withColumnRenamed("values", "valuesA")
              .withColumnRenamed("cardinality", "cardinalityA")
            val dsB = mapDS(fileB)
              .withColumnRenamed("dataset", "datasetB")
              .withColumnRenamed("colName", "attB")
              .withColumnRenamed("values", "valuesB")
              .withColumnRenamed("cardinality", "cardinalityB")


            val cross = dsA.alias("A").join(dsB.alias("B"), $"A.datasetA" < $"B.datasetB")
              .withColumn("sizeJoin", size(array_intersect($"valuesA", $"valuesB")))
              .withColumn("containmentA", $"sizeJoin" / $"cardinalityA")
              .withColumn("containmentB", $"sizeJoin" / $"cardinalityB")
              .select($"datasetA", $"attA", $"cardinalityA", $"datasetB", $"attB", $"cardinalityB", $"sizeJoin", $"containmentA", $"containmentB")


            cross.write.mode("overwrite").parquet(s"${outputPath}/${fileA}_${fileB}")

            val tmpdf = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
            if(tmpdf.count() == 0){
              println(s"***${fileA}_${fileB}: 0 ROWS ***")
            }
          } else {
            println(s"* ${count} out of ${numCombinations}: Already computed ground truth for ${fileA} and ${fileB}")

            try {
              val tmpdf = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
              if (tmpdf.count() == 0) {
                println(s"***${fileA}_${fileB}: 0 ROWS ***")
              }
            } catch {
              case e: org.apache.spark.sql.AnalysisException =>
                println(s"Failed to read the file: ${outputPath}/${fileA}_${fileB}")
//                e.printStackTrace() // Optionally print the full stack trace
                  val dsA = mapDS(fileA)
                    .withColumnRenamed("dataset", "datasetA")
                    .withColumnRenamed("colName", "attA")
                    .withColumnRenamed("values", "valuesA")
                    .withColumnRenamed("cardinality", "cardinalityA")
                val dsB = mapDS(fileB)
                  .withColumnRenamed("dataset", "datasetB")
                  .withColumnRenamed("colName", "attB")
                  .withColumnRenamed("values", "valuesB")
                  .withColumnRenamed("cardinality", "cardinalityB")


                val cross = dsA.alias("A").join(dsB.alias("B"), $"A.datasetA" < $"B.datasetB")
                  .withColumn("sizeJoin", size(array_intersect($"valuesA", $"valuesB")))
                  .withColumn("containmentA", $"sizeJoin" / $"cardinalityA")
                  .withColumn("containmentB", $"sizeJoin" / $"cardinalityB")
                  .select($"datasetA", $"attA", $"cardinalityA", $"datasetB", $"attB", $"cardinalityB", $"sizeJoin", $"containmentA", $"containmentB")


                cross.write.mode("overwrite").parquet(s"${outputPath}/${fileA}_${fileB}")

                val tmpdf = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
                if (tmpdf.count() == 0) {
                  println(s"***${fileA}_${fileB}: 0 ROWS ***")
                }else{
                  println(s"*** success writing ${fileA}_${fileB}")
                }
            }



          }


        }

        println("merging files....")
        val ground = comb.map { case (fileA, fileB) =>
          val dfG = spark.read.parquet(s"${outputPath}/${fileA}_${fileB}")
          val countRows = dfG.count()
          if(countRows == 0){
            println(s"-------> *** dataset ${fileA}_${fileB} contains no rows!!!! ***")
//            dfG.show(2)
            deleteFolderWithContent(s"${outputPath}/${fileA}_${fileB}")
          }
          dfG
        }.seq.reduce(_ union _)

        ground.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath+"csv")

        //        ground.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath+"csv")

        println("adding entities....")
        val groundWithEntities = setEntities(ground, sourceGraphs)
        groundWithEntities.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath+"csv")


      case 2 =>

        val ds = spark.read.option("header", "true").option("inferSchema", "true").csv(outputPath+"csv")
          .withColumn("semanticEntity", lit(0))
          .withColumn("semanticEntity", when(
            col("attA") === col("attB") && (
              (col("datasetA") === col("datasetB")) ||
                (substring_index(col("datasetA"), "_", 1) === substring_index(col("datasetB"), "_", 1))
              ),
            lit(1)
          ).otherwise(col("semanticEntity")))

        println(ds.count(),"***")
          ds.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath+"csv2")


      case 3 =>
        println("getting true ground entities")
        val groundEntity = spark.read.option("header", "true").option("inferSchema", "true")
          .csv(s"${basePath}/ground_semanticEntities.csv")
          .select("datasetA", "datasetB", "aliasParentA", "aliasParentB", "parentIRIA", "parentIRIB", "semanticEntity")
          .groupBy("datasetA", "datasetB", "aliasParentA", "aliasParentB", "parentIRIA", "parentIRIB")
          .agg(max(col("semanticEntity")).alias("semanticEntity"))
          .orderBy(col("semanticEntity").desc)

        println(groundEntity.count())

        groundEntity.distinct().repartition(1).write.mode("overwrite").option("header", true).csv( s"${basePath}/out_ground_parent")

    }



  }

}
