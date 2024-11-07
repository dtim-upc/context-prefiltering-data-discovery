package test_prepro.pipeline_new

import core.{SparkWrapper, VocabularyCore}
import groundTruth.CardinalityAndGraphs.{GraphAndFilename, loadSchemaGraphs_parallel}
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.sql.functions.{col, lower, max}

object GroundWithEntities extends SparkWrapper{


  case class PropertyInfo(alias:String, aliasParent: String, parentIRI: String, filename: String)

  def getPropertiesWithEntities( sourceGraphs: Map[Int, GraphAndFilename]): Seq[PropertyInfo] = {

    val query = s"SELECT ?alias ?aliasParent ?parentIRI WHERE { " +
      s" ?node <${RDF.`type`}> <${RDF.Property}> . " +
      s" ?node <${RDF.value}> ?alias . " +
      s" ?node <${RDFS.domain}> ?parentIRI ." +
      s" ?parentIRI <${RDF.value}> ?aliasParent . " +
      s"}"

    var proAndEntities = Seq.empty[PropertyInfo]
    for (g <- sourceGraphs) {

      val result_nodes = g._2.graph.runAQuery(query)
      while (result_nodes.hasNext) {
        val r = result_nodes.next()
        proAndEntities = proAndEntities :+ PropertyInfo( r.get("alias").toString, r.get("aliasParent").toString, r.get("parentIRI").toString, g._2.fileName )
      }


    }
    proAndEntities
  }


  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2"
    val graphsWithMetadataPath = s"${basePath}/graphsWithMetadata" // contains empty property metadata
    val outputPath = s"${basePath}/out_ground_parent"

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"${basePath}/datasetInfo.csv")


    val ground = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"${basePath}/ground.csv")
      .filter(col("containmentA") =!= 0)


    val option = 2

    option match {

      case 1 =>
        ground.printSchema()

        val parIdAndFileNames = dsInfo.select("id", "fileName").as[(Int, String)].collect().toList.par
        val sourceGraphs = loadSchemaGraphs_parallel(parIdAndFileNames, graphsWithMetadataPath)

        val proAndEntities = getPropertiesWithEntities(sourceGraphs)

        proAndEntities.foreach(println)

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
        dfWithParentsAB.show(false)


        println("Size ground", ground.count())
        println("size new ground", dfWithParentsAB.count)

        dfWithParentsAB.repartition(1).write.mode("overwrite").option("header", true).csv(outputPath)


      case 2 =>
        println("getting true ground entities")
        val groundEntity = spark.read.option("header", "true").option("inferSchema", "true")
          .csv(s"${basePath}/ground_semanticEntities.csv")
          .select("datasetA","datasetB","aliasParentA", "aliasParentB", "parentIRIA" ,"parentIRIB", "semanticEntity"  )
          .groupBy("datasetA","datasetB","aliasParentA", "aliasParentB", "parentIRIA" ,"parentIRIB")
          .agg(max(col("semanticEntity")).alias("semanticEntity"))
          .orderBy(col("semanticEntity").desc)

        println(groundEntity.count())

        groundEntity.distinct().repartition(1).write.mode("overwrite").option("header", true).csv(outputPath)


    }



  }

}
