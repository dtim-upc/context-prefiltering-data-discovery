package test_prepro.pipeline_v3

import core.{SparkWrapper, VocabularyCore}
import edu.upc.essi.dtim.nextiadi.jena.Graph
import io.github.haross.nuup.nextiajd.profiles.numeric.NumCardinality
import io.github.haross.nuup.nextiajd.profiles.str.Cardinality
import org.apache.jena.vocabulary.RDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import preprocessing.utils.Utils.createDirIfNotExists

import java.nio.file.{Files, Paths}
import scala.collection.parallel.ParSeq

// it will add a new triple in graphs to indicate a property is empty
object GraphsWithMetadata_03 extends SparkWrapper{

  import spark.implicits._

  val attColumn = "colName"
  case class GraphAndFilename( graph: Graph, fileName: String)
  case class GraphAndProfile( graph: Graph, profile: DataFrame, fileName: String)
  case class GraphAndProfiles( graph: Graph, strProfile: DataFrame, numProfile: Option[DataFrame],fileName: String)

  def loadSchemaGraphs(dsInfo: DataFrame, graphsPath: String): Map[Int,Graph] ={

    val sourceIDAndGraph = dsInfo.select("id", "fileName").collect().flatMap {
      case Row(id: Int, fileNameNoExt: String) =>
//        println(s"reading dataset graph ${fileNameNoExt}")
        val graph = new Graph()
        graph.loadModel(s"$graphsPath/${fileNameNoExt}.ttl")
        Some(id -> graph)
      case _ => None
    }.toMap

    sourceIDAndGraph
  }

  def loadSchemaGraphs_parallel_old(dsInfo: DataFrame, graphsPath: String):Unit ={


//    This does not work since Graph use Jena model which is not serializable...

//    val sourceIDAndGraph = dsInfo.select("id", "fileName").rdd.mapPartitions(rows => {
//      rows.flatMap {
//        case Row(id: Int, fileNameNoExt: String) =>
//          val graph = new Graph()
//          graph.loadModel(s"$graphsPath/${fileNameNoExt}.ttl")
//          Some(id -> graph)
//        case _ => None
//      }
//    }).collectAsMap().toMap //convert to an immutable Map
//
//    sourceIDAndGraph
  }


  def loadSchemaGraphs_parallel(parIdAndFileNames: ParSeq[(Int, String)], graphsPath: String): Map[Int,GraphAndFilename] ={

    val sourceIDAndGraph = parIdAndFileNames.flatMap { case (id, fileNameNoExt) =>
      val graph = new Graph()
      graph.loadModel(s"$graphsPath/${fileNameNoExt}.ttl")
      Some(id -> GraphAndFilename(graph, fileNameNoExt))
    }.seq.toMap // convert back to sequential collection

    sourceIDAndGraph

  }

  def loadRawProfiles_parallel(parIdAndFileNames: ParSeq[(Int, String)], profilesPath: String): Map[Int,DataFrame] = {

    val sourceIDAndProfile = parIdAndFileNames.flatMap { case (id, fileNameNoExt) =>
      val profileDf =  spark.read.parquet(s"${profilesPath}/${fileNameNoExt}")
      Some(id -> profileDf)
    }.seq.toMap

    sourceIDAndProfile

  }

  def loadSchemaGraphsAndProfiles(parIdAndFileNames: ParSeq[(Int, String)], graphsPath: String, profilesPath: String, profilesNumPath: String): ParSeq[(Int, GraphAndProfiles)] = {

    val sourceIDAndGraph = parIdAndFileNames.flatMap { case (id, fileNameNoExt) =>
      val graph = new Graph()
      graph.loadModel(s"$graphsPath/${fileNameNoExt}.ttl")

      val strProfileDF =  spark.read.parquet(s"${profilesPath}/${fileNameNoExt}")

      // if there is no num profile directory, we assume ds does not have num attributes.
      val path = Paths.get(s"${profilesNumPath}/${fileNameNoExt}")
      if (Files.exists(path)) {

        val numProfileDF = spark.read.parquet(s"${profilesNumPath}/${fileNameNoExt}")
        Some(id -> GraphAndProfiles(graph, strProfileDF, Some(numProfileDF), fileNameNoExt))
      } else {
        Some(id -> GraphAndProfiles(graph, strProfileDF, None, fileNameNoExt))
      }

    }.seq.par
//      .toMap // convert back to sequential collection
    sourceIDAndGraph
//    sourceIDAndGraph

  }

  def loadSchemaGraphsAndNumProfiles(parIdAndFileNames: ParSeq[(Int, String)], graphsPath: String, profilesPath: String): ParSeq[(Int, GraphAndProfile)] = {

    val sourceIDAndGraph = parIdAndFileNames.flatMap { case (id, fileNameNoExt) =>
      val graph = new Graph()
      graph.loadModel(s"$graphsPath/${fileNameNoExt}.ttl")
      val path = Paths.get(s"${profilesPath}/${fileNameNoExt}")
      if ( Files.exists(path)) {
        val profileDf = spark.read.parquet(s"${profilesPath}/${fileNameNoExt}")
        Some(id -> GraphAndProfile(graph, profileDf, fileNameNoExt))
      } else {
        println(s"excluding datasource ${fileNameNoExt} since there is no numeric profile")
        None
      }

    }.seq.par
    //      .toMap // convert back to sequential collection
    sourceIDAndGraph
    //    sourceIDAndGraph

  }


  def encodeStrProfile(df: DataFrame): Unit = {




  }


  def setProfileStrAtts(gAndProfiles: ParSeq[(Int, GraphAndProfiles)], graphsPath: String): Unit = {

    gAndProfiles.foreach{ case(id, gAndP) =>

      val graph = gAndP.graph
      val profileStr = gAndP.strProfile



    }


  }

  def setEmptyProperties(gAndProfiles: ParSeq[(Int, GraphAndProfiles)], graphsPath: String): Unit = {

    gAndProfiles.foreach { case (id, gAndP) =>

      val graph = gAndP.graph
      val profiledf = gAndP.strProfile

      val cardinalitesStr = profiledf.filter(col(Cardinality.nameAtt) === "0")
        .select(attColumn, Cardinality.nameAtt).as[(String, Int)].collect().toList

      var cardinalitiesNum = List.empty[(String, Int)]
      if(gAndP.numProfile.nonEmpty){
        println("processing numeric ")
        cardinalitiesNum = gAndP.numProfile.get.filter(col(NumCardinality.nameAtt) === "0")
          .select(attColumn, NumCardinality.nameAtt).as[(String, Int)].collect().toList

      }
      val cardinalites = cardinalitesStr ++ cardinalitiesNum


      if(cardinalites.nonEmpty) {
          cardinalites.par.foreach{ case (alias, cardinality) =>

            val query = "SELECT ?node WHERE { " +
              s" ?node <${RDF.`type`}> <${RDF.Property}> . " +
              s" ?node <${RDF.value}> '${alias}' . " +
              " }"

            val result = graph.runAQuery(query)
            if(result.hasNext){
              val r = result.next()
              val node = r.getResource("node").getURI
              println(s"inserting triple for empty property ${node} and alias ${alias}")
              graph.add(node, VocabularyCore.metadata, VocabularyCore.emptyAtt )
            } else {
              println("There is a problem. Given alias cannot be found in the source graph. ")
            }

          }
      }
      graph.write(s"${graphsPath}/${gAndP.fileName}.ttl","ttl")

    }

  }




  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2"
    val graphPath = s"${basePath}/graphs"
    val rawStrProfilesPath =  s"${basePath}/profiles_new"
    val rawNumProfilesPath =  s"${basePath}/numProfiles_new"
    val newGraphsPath =  s"${basePath}/graphsWithMetadata"

    val profilePathNormByDS = s"${basePath}/profiles_normByDS"
    val numProfilePathNormByDS = s"${basePath}/numProfiles_normByDS"

    createDirIfNotExists(newGraphsPath)

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"${basePath}/datasetInfo.csv")

    val parIdAndFileNames = dsInfo.select("id", "fileName").as[(Int, String)].collect().toList.par

    val gAndRawProfiles = loadSchemaGraphsAndProfiles(parIdAndFileNames, graphPath, rawStrProfilesPath, rawNumProfilesPath)
    setEmptyProperties(gAndRawProfiles, newGraphsPath)


//    println("\n num profiles...")
//    val gAndNumProfiles = loadSchemaGraphsAndNumProfiles(parIdAndFileNames, graphPath, rawNumProfilesPath)
//    setEmptyNumProperties(gAndNumProfiles, newGraphsPath)



  }

}
