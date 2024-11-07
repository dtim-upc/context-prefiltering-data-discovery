package groundTruth

import core.SparkWrapper
import edu.upc.essi.dtim.nextiadi.jena.Graph
import groundTruth.CardinalityAndGraphs.spark
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.parallel.ParMap

object GraphStatistics extends SparkWrapper {

  def loadSchemaGraphs(dsInfo: DataFrame, graphsPath: String): Map[Int, Graph] = {

    val sourceIDAndGraph = dsInfo.select("id", "fileName").collect().flatMap {
      case Row(id: Int, fileNameNoExt: String) =>
        println(s"reading dataset graph ${fileNameNoExt}")
        val graph = new Graph()
        graph.loadModel(s"$graphsPath/${fileNameNoExt}.ttl")
        Some(id -> graph)
      case _ => None
    }.toMap

    sourceIDAndGraph
  }

  def getNumberEntities(graph: Graph, fileName: String): (String, Int) = {
    val res = graph.runAQuery(s" SELECT (COUNT(DISTINCT ?entity) as ?entityCount) WHERE { ?entity <${RDF.`type`}> <${RDFS.Class}>. } ")
    if (res.hasNext) {
      val r = res.next()
      (fileName, r.get("entityCount").asLiteral().getInt)
    } else {
      print("error!!!!")
      (fileName, 0)
    }
  }


  def getNumberEntityRelationships(graph: Graph, fileName: String): (String, Int) = {
    // Adjust the SPARQL query to count DISTINCT rdf:property with a range not being xsd literal
    val query =
      s"""
      SELECT (COUNT(DISTINCT ?property) as ?propertyCount)
      WHERE {
        ?property <${RDF.`type`}> <${RDF.Property}> .
        ?property <${RDFS.range}> ?range .
        FILTER(!STRSTARTS(STR(?range), "http://www.w3.org/2001/XMLSchema#"))
      }
    """
    val res = graph.runAQuery(query)
    if (res.hasNext) {
      val r = res.next()
      (fileName, r.get("propertyCount").asLiteral().getInt) // Assuming propertyCount can be converted directly to Int
    } else {
      (fileName, 0)
    }
  }

  def getNumberAttributes(graph: Graph, fileName: String): (String, Int) = {
    // Adjust the SPARQL query to count DISTINCT rdf:property with range as xsd literal
    val query =
      s"""
      SELECT (COUNT(DISTINCT ?property) as ?propertyCount)
      WHERE {
        ?property <${RDF.`type`}> <${RDF.Property}> .
        ?property <${RDFS.range}> ?range .
        FILTER(STRSTARTS(STR(?range), "http://www.w3.org/2001/XMLSchema#"))
      }
    """
    val res = graph.runAQuery(query)
    if (res.hasNext) {
      val r = res.next()
      (fileName, r.get("propertyCount").asLiteral().getInt) // Assuming propertyCount can be converted directly to Int
    } else {
      (fileName, 0)
    }
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._

//    val basePath = "/Users/javierflores/Koofr/PhD/code/result/data"
    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2"
    val graphPath = s"${basePath}/graphs"

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"${basePath}/datasetInfo.csv")

    val parIdAndFileNames = dsInfo.select("id", "fileName").as[(Int, String)].collect().toList
      .par // Convert list to parallel collection

    val graphs = loadSchemaGraphs(dsInfo, graphPath)

    val results: ParMap[String, Int] = graphs.par.map(g => getNumberEntities(g._2, parIdAndFileNames.find(_._1 == g._1).map(_._2).get)).toMap

    if (results.nonEmpty) {
      val (fileWithMostEntities, maxEntities) = results.maxBy(_._2)
      val (fileWithLestEntities, minEntities) = results.minBy(_._2)
      val totalEntities = results.values.sum

      println(s"$fileWithMostEntities has the most entities: $maxEntities")
      println(s"$fileWithLestEntities has the least entities: $minEntities")
      println(s"Total number of entities across all datasets: $totalEntities")
      println("------")
    } else {
      println("No results to display.")
    }


    val results2: ParMap[String, Int] = graphs.par.map(g => getNumberAttributes(g._2, parIdAndFileNames.find(_._1 == g._1).map(_._2).get)).toMap

    if (results2.nonEmpty) {
      val (fileWithMostProperties, maxProperties) = results2.maxBy(_._2)
      val (fileWithLeastProperties, minProperties) = results2.minBy(_._2)
      val totalProperties = results2.values.sum

      println(s"$fileWithMostProperties has the most attributes: $maxProperties")
      println(s"$fileWithLeastProperties has the least attributes: $minProperties")
      println(s"Total number of attributes: $totalProperties")
      println("------")
    } else {
      println("No results to display.")
    }


    val results3: ParMap[String, Int] = graphs.par.map(g => getNumberEntityRelationships(g._2, parIdAndFileNames.find(_._1 == g._1).map(_._2).get)).toMap

    if (results3.nonEmpty) {
      val (fileWithMostProperties, maxProperties) = results3.maxBy(_._2)
      val totalProperties = results3.values.sum

      println(s"$fileWithMostProperties has the most entity relationships: $maxProperties")
      println(s"Total number of entity relationships across all datasets: $totalProperties")
    } else {
      println("No results to display.")
    }

  }

}
