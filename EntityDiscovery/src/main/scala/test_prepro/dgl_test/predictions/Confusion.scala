package test_prepro.pipeline_new.predictions

import core.SparkWrapper
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions.{col, desc, first, udf, when}
import org.apache.spark.sql.types.DoubleType

object Confusion extends SparkWrapper{


  def main(args: Array[String]): Unit = {

//    val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/TEST3/str_num_inv/"
//    val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/TEST3//str_inv/"
//    val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/Test_last_bool2/out/"
//   **     val basePath = "/Users/javierflores/Documents/upc/projects/MECCH/data/Test_1million/out/"
//    val path = s"${basePath}predEntitiesContainment/part-00000-384bc26f-b6f6-48fb-962c-e806672818fc-c000.csv"
//    val path = s"${basePath}predEntitiesContainment/part-00000-8f409428-3b34-4420-94e8-989894044392-c000.csv"
//    *** val path = s"${basePath}predEntitiesContainment/part-00000-c8448a08-8af4-4d82-be54-aa89a072dc5b-c000.csv"



//      val path = "/Users/javierflores/Downloads/pruebas/tmp3/output.csv"



//      val path = s"${basePath}withConstraintPrueba/part-00000-f1fba48f-b0a8-452c-90ea-edd5699c2d45-c000.csv"
    val sigmoidUdf = udf((x: Double) => 1 / (1 + scala.math.exp(-x)))



      val colTrueLabel = "semanticEntity"
//      val colTrueLabel = "syntactic"
//    val data = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
val data =  spark.read.parquet("/Users/javierflores/Downloads/pruebas/tmp2/newlink").drop("link").withColumnRenamed("link_new", "link")
//      .withColumn("syntactic", when(col("containmentA") >= 0.1 || col("containmentB") >= 0.1 ,1  ).otherwise(0)  )
      .withColumnRenamed(colTrueLabel, "trueLabel")
//      .withColumn("score", sigmoidUdf(col("score")))
//      .withColumn("link", when(col("score") >= 0.6,1  ).otherwise(0)  )
//      .withColumn("link", when(col("score") >= 0.7,1  ).otherwise(0)  )
      .withColumnRenamed("link", "prediction")

      println(s"number of semantic entities: ${data.where(col("trueLabel")===1).count()}")

      data.printSchema()

//    val dfSorted = df.sort(desc("score"))
//
//    val data = dfSorted
//      .groupBy("datasetA", "datasetB", "aliasParentA")
//      .agg(
////        first("aliasParentA").as("aliasParentA"),
//        first("aliasParentB").as("aliasParentB"),
//        first("parentIRIA").as("parentIRIA"),
//        first("parentIRIB").as("parentIRIB"),
//        first("score").as("score"),
//        first("prediction").as("prediction"),
//        first("containmentA").as("containmentA"),
//        first("containmentB").as("containmentB"),
//        first("trueLabel").as("trueLabel"))
////      .union(dfSorted.filter(col("trueLabel") === 1)).distinct()


    val tp = data.filter("prediction = 1 AND trueLabel = 1").count()
    val tn = data.filter("prediction = 0 AND trueLabel = 0").count()
    val fp = data.filter("prediction = 1 AND trueLabel = 0").count()
    val fn = data.filter("prediction = 0 AND trueLabel = 1").count()

    // Create the confusion matrix as a 2x2 array
    val confusionMatrix = Array(Array(tn, fp), Array(fn, tp))

    // Print the confusion matrix
    println("Confusion Matrix:")
    println("\t\tPredicted 0\tPredicted 1")
    println(s"Actual 0\t\t${confusionMatrix(0)(0)}\t\t${confusionMatrix(0)(1)}")
    println(s"Actual 1\t\t${confusionMatrix(1)(0)}\t\t${confusionMatrix(1)(1)}")


    val precision = tp.toDouble / (tp + fp)
    val recall = tp.toDouble / (tp + fn)
    val f1Score = 2 * (precision * recall) / (precision + recall)
    val accuracy = (tp + tn).toDouble / (tp + tn + fp + fn)

    println("\n----------METRICS---------")
    println("Accuracy: " + accuracy)
    println("Precision: " + precision)
    println("Recall: " + recall)
    println("F1-score: " + f1Score)


    val rdd = data.select(col("prediction").cast(DoubleType), col("trueLabel").cast(DoubleType)).rdd.map {
      case row =>
        (row.getAs[Double]("prediction"), row.getAs[Double]("trueLabel"))
    }

    // Create BinaryClassificationMetrics object
    val metrics = new BinaryClassificationMetrics(rdd)

    // Compute AUROC
    val auroc = metrics.areaUnderROC()
    println(s"AUROC: ${auroc}")


  }



}
