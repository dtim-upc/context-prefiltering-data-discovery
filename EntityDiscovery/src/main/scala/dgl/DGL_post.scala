package dgl

import core.SparkWrapper
import dgl.Commons.{writeInverseLinksFile, writeLinksFile, writeNodeFilesPost}
import edu.upc.essi.dtim.nextiadi.jena.Graph
import groundTruth.CardinalityAndGraphs.{GraphAndFilename, loadSchemaGraphs_parallel}
import org.apache.jena.vocabulary.{RDF, RDFS}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.col
import preprocessing.utils.Utils.{createDirIfNotExists, writeFile}

import scala.collection.parallel.ParSeq
import scala.util.Random
import scala.util.matching.Regex

object DGL_post extends SparkWrapper{

  import spark.implicits._

  // check if some ids are missing by comparing the max id and the number of distinct ids
  def validateIDs(df:DataFrame, typeNode: String): Unit = {

    val maxId = df.agg(functions.max(col("node_id"))).head.getInt(0)
    val countUniqueIds = df.select("node_id").distinct().count()

    // since ids start at 0, we add + 1
    if (maxId + 1 == countUniqueIds) {
//      println("No missing ids")
    } else {
      println(s"-----> ERROR: There may be missing ids for ${typeNode} nodes")
    }

    val nonPositiveCount = df.filter(col("node_id") < 0).count()

    if (nonPositiveCount > 0) {
      println(s"-----> ERROR: There are negative node_id values for ${typeNode}")
      df.filter(col("node_id") < 0).select("node_id")
    }

  }


  def loadNodes(entityNodesPath: String, strNodesPath: String): (Seq[NodeClass], Seq[NodeStrPost] )= {

    val entityDf = spark.read.option("header", "true").option("inferSchema", "true").csv(entityNodesPath)
    val strDf = spark.read.option("header", "true").option("inferSchema", "true").csv(strNodesPath)

    validateIDs(entityDf, "entity")
    validateIDs(strDf, "str")

    val entityNodes = entityDf.collect().map(row =>
      NodeClass(
        row.getAs[Int]("node_id"),
        row.getAs[String]("node_iri"),
        row.getAs[String]("node_name"),
        row.getAs[String]("alias"),
        row.getAs[Int]("ds_id"),
        row.getAs[String]("ds_name")
      )
    ).toSeq

    val strNodes = strDf.collect().map(row =>

      NodeStrPost(
        row.getAs[Int]("node_id"),
        row.getAs[String]("node_iri"),
        row.getAs[String]("node_name"),
        row.getAs[Int]("ds_id"),
        row.getAs[String]("ds_name") ,
        row.getAs[String]("node_attributes"),
        row.getAs[Int]("domain_id"),
      )
    ).toSeq

    println(s"number entity nodes: ${entityNodes.size}")
    println(s"number str nodes: ${strNodes.size} ")

    (entityNodes, strNodes)

  }

  def createLinksFromDF(df: DataFrame): Seq[Link] = {

    df.collect().map(row => {
      Link(
        row.getAs[Int]("src_id"),
        row.getAs[Int]("dst_id"),
        row.getAs[String]("str_type"),
        row.getAs[Int]("link_weight").toString,
      )
    }).toSeq


  }

  def loadLinks(strLinksPath: String, relationshipsPath:String, aligPath: String): (Seq[Link], Seq[Link],  Seq[Link]) = {

    val strLinksDf = spark.read.option("header", "true").option("inferSchema", "true").csv(strLinksPath)
    val relLinksDf = spark.read.option("header", "true").option("inferSchema", "true").csv(relationshipsPath)
    val aligDf = spark.read.option("header", "true").option("inferSchema", "true").csv(aligPath)

    validateIDs(strLinksDf, "strLinksDf_src", "src_id")
    validateIDs(strLinksDf, "strLinksDf_dst", "dst_id")
    validateIDs(relLinksDf, "relLinksDf_src", "src_id")
    validateIDs(relLinksDf, "relLinksDf_dst", "dst_id")
    validateIDs(aligDf, "aligDf_src", "src_id")
    validateIDs(aligDf, "aligDf_dst", "dst_id")

    val strLinks = createLinksFromDF( strLinksDf)
    val relLinks = createLinksFromDF( relLinksDf )
    val aligLinks = createLinksFromDF( aligDf )

    println(s"number str links ${strLinks.size}")
    println(s"number rel links ${relLinks.size}")
    println(s"number alig links ${aligLinks.size}") //2170

//    number entity nodes: 1096
//    number str nodes: 3893
//    number str links
//    3893
//    number rel links
//    984
//    number alig links
//    2170

    (strLinks, relLinks, aligLinks)

  }


  def splitAlignmentsLinks(alignmentsLinks: Seq[Link]): (Seq[Link], Seq[Link], Seq[Link]) = {
    // sample alignments links
    val seed = 1023
    val random = new Random(seed)
    // we will take 30%: 15% for validation and 15% for testing
    val percent = (alignmentsLinks.size * 0.3).toInt
    val sampleValAndTest = random.shuffle(alignmentsLinks).take(percent)
    val percentTest = (sampleValAndTest.size * 0.5).toInt

    val alignmentsTest = sampleValAndTest.take(percentTest)
    val alignmentsVal = sampleValAndTest.diff(alignmentsTest)
    val alignmentsTraining = alignmentsLinks.diff(sampleValAndTest)

    val overlappingTestVal = alignmentsTest.intersect(alignmentsVal)
    val overlappingTrainVal = alignmentsTraining.intersect(alignmentsVal)
    val overlappingTrainTest = alignmentsTraining.intersect(alignmentsTest)

    println(s"Overalpping test - val: ${overlappingTestVal.size}")
    println(s"Overalpping train - val: ${overlappingTrainVal.size}")
    println(s"Overalpping train - test: ${overlappingTrainTest.size}")


    println(s"initial size: ${alignmentsLinks.size}")
    println(s"test size: ${alignmentsTest.size}")
    println(s"val size: ${alignmentsVal.size}")
    println(s"train size: ${alignmentsTraining.size}")

    (alignmentsTraining, alignmentsVal, alignmentsTest)
  }


  def generateNegativeAlignments(sourceGraphs: Map[Int,GraphAndFilename], entity: NodeClass, numberAlign: Int, generator:String, entityIriAndIDs: Map[String, Int]): Seq[Link] = {


    generator match {

      case "negRelationalSampling" =>

        val negAlig = generateNegativeRelationalSampling(sourceGraphs(entity.dsId).graph, entity, numberAlign, entityIriAndIDs  )
        if(negAlig.size < numberAlign) {
          println(s"----> WARNING : Cannot generate more negative alignments as there are no more classes for relational sampling in source ${entity.dsName}. Generated ${negAlig.size} but requested ${numberAlign}")
          val missingNAlig =  numberAlign - negAlig.size

//          val regex: Regex = "(.*)_([0-9]+)$".r
//          entity.dsName match {
//            case regex(text, num) =>
//              if(num >= 1){
//
//              } else {
//                val negAligRemaining = generateNegativeRelationalSampling(sourceGraphs(entity.dsId).graph, entity, numberAlign, entityIriAndIDs  )
//              }
//
//            case _ => throw new IllegalArgumentException("Invalid input string format")
//          }

          // for now we are just duplicating...
          return negAlig ++ negAlig.take(missingNAlig)

        }
        negAlig
      case _ =>
        println("there is no default option for negative alignments")
        Seq.empty[Link]
    }

  }


  // for a positive pair (e1, e2), sample (e1, e') as a negative pair
  // where e' associates with e1 via Entity-Relationship-Entity metapath (i.e., a n-hop neighbor)
  // and (e1, e') does not exist in the original graph
  // We don't check the metapath since bootstrapping connects all classes with relationships. So all classes from the schema are connected
  // create a negative link between resources of the same bootstrapping graph
  def generateNegativeRelationalSampling(graph: Graph, baseEntity: NodeClass, nAlignments:Int, entityIriAndIDs: Map[String, Int]): Seq[Link] = {
    var links = Seq.empty[Link]
    val query = s"SELECT ?node WHERE { " +
      s" ?node <${RDF.`type`}> <${RDFS.Class}>  " +
      s" FILTER(?node != <${baseEntity.iri}>) " + // remove e1 from retrieve entities
      s" } LIMIT ${nAlignments} "

    val result_nodes = graph.runAQuery(query)
    var entities = Seq.empty[String]
    while (result_nodes.hasNext) {
      val r = result_nodes.next()
      entities = entities :+ r.get("node").toString
    }

    for (e_prime <- entities) {
        links = links :+ Link(baseEntity.id, getIDNode(e_prime, entityIriAndIDs), Commons.linkTypes("entityAlignment"), "NegativeEntityAlignment")
    }

    links
  }

  def getIDNode(iri: String, nodes: Map[String, Int], defaultValue: Int = -1, nodeType: String = "class"): Int = {

    nodes.get(iri) match {
      case Some(id) =>
        id
      case None =>
        println(s"---- ERROR: No ${nodeType} node id found with IRI: $iri----")
        defaultValue
    }

  }

  def generateNegatives(alignmentsVal: Seq[Link], entityDict: Map[Int, NodeClass], sourceGraphs: Map[Int, GraphAndFilename], entityIriAndIDs: Map[String, Int], typeSet :String): Seq[Link] = {

    // Create negatives for validation. For n positive alignments, we select n negative alignments.
    //    Example: if we have 2 positives with src_id 3,
    //    we have to select 2 negatives with the same src_id and different target that are negatives
    val idAndNumAlig = alignmentsVal.groupBy(_.src_id).mapValues(_.size).toList
//    println("Nodes ids and counts in validation: " + idAndNumAlig.toSeq)

    var negativeAlignmentsVal = Seq.empty[Link]
    for ((e1, count) <- idAndNumAlig) {

      val entity = entityDict(e1)
      val graph = sourceGraphs.filterKeys(_ == entity.dsId)
      val negativeGenerator = "negRelationalSampling"
      val negAlignmentsForEntity = generateNegativeAlignments(graph, entity, count, negativeGenerator, entityIriAndIDs);
      negativeAlignmentsVal = negativeAlignmentsVal ++ negAlignmentsForEntity
    }
    val val_neg_edges = negativeAlignmentsVal.sortBy(link => (link.src_id, link.target_id))

    println(s"number of ${typeSet} alignments ${alignmentsVal.size}")
    println(s"number of ${typeSet} - negative alignments: ${val_neg_edges.size} ")
    val_neg_edges
  }


  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/data"
    val prePath = s"${basePath}/pre_nodes_links"
    val strLinksPath = s"${prePath}/str_links.csv"
    val relLinksPath = s"${prePath}/rel_links.csv"
    val alignmentsLinksPath = s"${prePath}/alignments.csv"
    val attNodesPath = s"${prePath}/strNodes.csv"
    val entityNodesPath = s"${prePath}/entityNodes.csv"
    val outputFolder = s"${basePath}/input_dgl"

    createDirIfNotExists(outputFolder)

    val (entityNodes, strNodes) = loadNodes(entityNodesPath, attNodesPath)
    val (strLinks, relLinks, aligLinks) = loadLinks(strLinksPath, relLinksPath, alignmentsLinksPath)


    val (alignmentsTraining, alignmentsVal, alignmentsTest) = splitAlignmentsLinks(aligLinks)


    // we sort the alignments by src_id to get the positions on the adjacency matrix.
    val alignmentsAdjM = aligLinks.sortBy(link => (link.src_id, link.target_id))
    val train_idx = alignmentsTraining.map(link => alignmentsAdjM.indexOf(link)).sorted
    val val_idx = alignmentsVal.map(link => alignmentsAdjM.indexOf(link)).sorted
    val test_idx = alignmentsTest.map(link => alignmentsAdjM.indexOf(link)).sorted


    val graphsWithMetadataPath = s"${basePath}/graphsWithMetadata" // contains empty property metadata
    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"${basePath}/datasetInfo.csv")
      .filter(col("active") === true)
      .filter(col("testing2") === false)
    val parIdAndFileNames = dsInfo.select("id", "fileName").as[(Int, String)].collect().toList.par
    val entityDict = entityNodes.map( node => (node.id -> node) ).seq.toMap
    val entityIriAndIDs = entityNodes.map(node => (node.iri -> node.id)).seq.toMap



    val sourceGraphs = loadSchemaGraphs_parallel(parIdAndFileNames, graphsWithMetadataPath)
    val val_neg_align = generateNegatives(alignmentsVal, entityDict, sourceGraphs, entityIriAndIDs, "val")
    // obtain negatives for test
    val test_neg_align =  generateNegatives(alignmentsTest, entityDict, sourceGraphs, entityIriAndIDs, "test")

    writeNodeFilesPost(entityNodes, strNodes, outputFolder)
    writeLinksFile(relLinks, outputFolder, "g_rel_links.csv")
    writeLinksFile(strLinks, outputFolder, "g_attr_links.csv")
    writeInverseLinksFile(strLinks.sortBy(_.target_id), outputFolder, "g_attr_inv_links.csv")
    // not sure if I should order links. for now leave it
    writeLinksFile(alignmentsTraining, outputFolder, "g_train_alignments_links.csv")
    writeLinksFile( alignmentsTraining ++ alignmentsVal , outputFolder, "g_val_alignments_links.csv")
    writeLinksFile( alignmentsTraining ++ alignmentsVal ++ alignmentsTest , outputFolder, "g_test_alignments_links.csv")


    var helperContent = "key,value\n"

        //    Seq(node.id,node.name,node.iri,"\""+node.profile.getOrElse("NOT FOUND")).mkString(",")+"\"" + "\n"

    helperContent = helperContent + "test_neg_edges,\""+test_neg_align.map( x => s"${x.src_id},${x.target_id}"  ).mkString(";")+"\"\n"
    helperContent = helperContent + "val_neg_edges,\""+val_neg_align.map( x => s"${x.src_id},${x.target_id}" ).mkString(";")+"\"\n"
    helperContent = helperContent + "train_idx,\""+train_idx.mkString(",")+"\"\n"
    helperContent = helperContent + "val_idx,\""+val_idx.mkString(",")+"\"\n"
    helperContent = helperContent + "test_idx,\""+test_idx.mkString(",")+"\"\n"

    writeFile(helperContent, outputFolder, "helpers.csv" )

    validateFiles()
  }


  def validateFiles(): Unit = {

    val outputFolder = "/Users/javierflores/Koofr/PhD/code/result/data/input_dgl"
    val strNodesPath = s"${outputFolder}/strNodes.csv"
    val entityNodesPath = s"${outputFolder}/entityNodes.csv"
    val g_attr_linksPath = s"${outputFolder}/g_attr_links.csv"
    val g_attr_inv_linksPath = s"${outputFolder}/g_attr_inv_links.csv"
    val g_rel_linksPath = s"${outputFolder}/g_rel_links.csv"
    val g_test_alignments_linksPath = s"${outputFolder}/g_test_alignments_links.csv"
    val g_train_alig_Path = s"${outputFolder}/g_train_alignments_links.csv"
    val g_val_alig_Path = s"${outputFolder}/g_val_alignments_links.csv"

    val entityDf = spark.read.option("header", "true").option("inferSchema", "true").csv(entityNodesPath)
    val strDf = spark.read.option("header", "true").option("inferSchema", "true").csv(strNodesPath)
    val g_attr_linksDf = spark.read.option("header", "true").option("inferSchema", "true").csv(g_attr_linksPath)
    val g_attr_inv_linksDf = spark.read.option("header", "true").option("inferSchema", "true").csv(g_attr_inv_linksPath)
    val g_rel_linksPathDf = spark.read.option("header", "true").option("inferSchema", "true").csv(g_rel_linksPath)
    val g_test_alignments_linksDf = spark.read.option("header", "true").option("inferSchema", "true").csv(g_test_alignments_linksPath)
    val g_train_alig_Df = spark.read.option("header", "true").option("inferSchema", "true").csv(g_train_alig_Path)
    val g_val_alig_Df = spark.read.option("header", "true").option("inferSchema", "true").csv(g_val_alig_Path)

    validateIDs(entityDf, "entity", "node_id", true)
    validateIDs(strDf, "str", "node_id", true)

    validateIDs(g_attr_linksDf, "g_attr_src", "src_id")
    validateIDs(g_attr_linksDf, "g_attr_dst", "dst_id")

    validateIDs(g_attr_inv_linksDf, "g_attr_inv_src", "src_id")
    validateIDs(g_attr_inv_linksDf, "g_attr_inv_dst", "dst_id")

    validateIDs(g_rel_linksPathDf, "g_rel_src", "src_id")
    validateIDs(g_rel_linksPathDf, "g_rel_dst", "dst_id")

    validateIDs(g_test_alignments_linksDf, "g_test_alig_src", "src_id")
    validateIDs(g_test_alignments_linksDf, "g_test_alig_dst", "dst_id")

    validateIDs(g_train_alig_Df, "g_train_src", "src_id")
    validateIDs(g_train_alig_Df, "g_train_dst", "dst_id")

    validateIDs(g_val_alig_Df, "g_val_src", "src_id")
    validateIDs(g_val_alig_Df, "g_val_dst", "dst_id")

  }

  def validateIDs(df: DataFrame, typeNode: String, colName: String, uniqueIds:Boolean = false): Unit = {

    if(uniqueIds){
      val maxId = df.agg(functions.max(col(colName))).head.getInt(0)
      val countUniqueIds = df.select(colName).distinct().count()

      // since ids start at 0, we add + 1
      if (maxId + 1 == countUniqueIds) {
        //      println("No missing ids")
      } else {
        println(s"-----> ERROR: There may be missing ids for ${typeNode} nodes")
      }
    }


    val nonPositiveCount = df.filter(col(colName) < 0).count()

    if (nonPositiveCount > 0) {
      println(s"-----> ERROR: There are negative node_id values for ${typeNode}")
      df.filter(col(colName) < 0).select(colName)
    }

  }

}
