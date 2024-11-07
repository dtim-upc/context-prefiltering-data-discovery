package test_prepro.pipeline_new.predictions

import core.SparkWrapper
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, max, when}

object PredictionsPrepro extends SparkWrapper{

  def main(args: Array[String]): Unit = {

//    val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/TEST3/str_num_inv/"
//val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/TEST3/str_inv/"
//      val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/Test_last_bool2/out/"
    val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/Test_1million/out/"

    val predictionsPath = s"${basePath}predictions.csv"
    val entityNodesPath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2/input_dgl_num_str/entityNodes.csv"

    val predictionsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(predictionsPath)
      .withColumn("link", when(col("score") >= 0.5, 1).otherwise(0))

    val entityDF = spark.read.option("header", "true").option("inferSchema", "true").csv(entityNodesPath)


    val entitySRCDF = entityDF.select("node_id","alias","node_iri")
      .withColumnRenamed("alias", "alias_src")
      .withColumnRenamed("node_iri","node_iri_src")

    val entityDSTDF = entityDF.select("node_id","alias","node_iri")
      .withColumnRenamed("alias", "alias_dst")
      .withColumnRenamed("node_iri", "node_iri_dst")


    val dfWithSrc = predictionsDF.join(entitySRCDF, predictionsDF("src") ===  entitySRCDF("node_id") , "left").drop("node_id")
    val dfWithSrcDst = dfWithSrc.join(entityDSTDF, dfWithSrc("dst") ===  entityDSTDF("node_id"), "left" ).drop("node_id")


//    println(s"entityDF ${predictionsDF.count()}")
//    println(s"df ${dfWithSrcDst.count()}")


    dfWithSrcDst.repartition(1).write.mode("overwrite").option("header", true).csv(s"${basePath}predInfo")



    val dfGround = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/javierflores/Koofr/PhD/code/result/dataTest_v2/ground_semanticEntities.csv")


    val dfWithSrcDstPred = dfWithSrcDst.select("score","link", "node_iri_src", "node_iri_dst")
    val groundPred  = dfGround.join(dfWithSrcDstPred, dfGround("parentIRIA") === dfWithSrcDstPred("node_iri_src") && dfGround("parentIRIB") === dfWithSrcDstPred("node_iri_dst"),"left" )
      .drop("node_iri_src","node_iri_dst")


    groundPred.repartition(1).write.mode("overwrite").option("header", true).csv(s"${basePath}groundPredInfo")


    val groundEntitiesAndMaxContainment = groundPred
      .select("datasetA", "datasetB", "aliasParentA", "aliasParentB", "parentIRIA", "parentIRIB", "score","link","semanticEntity", "containmentA","containmentB")
      .groupBy("datasetA", "datasetB", "aliasParentA", "aliasParentB", "parentIRIA", "parentIRIB", "score","link")
      .agg(
        functions.max(col("containmentA")).alias("containmentA"),
        functions.max(col("containmentB")).alias("containmentB"),
        functions.max(col("semanticEntity")).alias("semanticEntity")
      )

    groundEntitiesAndMaxContainment.repartition(1).write.mode("overwrite").option("header", true).csv(s"${basePath}predEntitiesContainment")



  }

}
