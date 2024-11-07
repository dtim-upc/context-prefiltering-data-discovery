package dgl.num

import core.{SparkWrapper, VocabularyCore}
import dgl.{Commons, IdAndFile, Link, NodeClass, NodeNum, NodeStr, ProfilesNumRow, ProfilesRow}
import dgl.Commons.{writeLinksFile, writeNodeFiles}
import edu.upc.essi.dtim.nextiadi.jena.Graph
import groundTruth.CardinalityAndGraphs.{GraphAndProfile, GraphAndProfiles}
import io.github.haross.nuup.nextiajd.NextiaJD
import io.github.haross.nuup.nextiajd.profiles.numeric.distribution.percentiles.{IQR, Quartile1, Quartile2, Quartile3, Quartile4}
import io.github.haross.nuup.nextiajd.profiles.numeric.distribution.{CoefficientVariation, Kurtosis, MAD, MedAD, Skewness}
import io.github.haross.nuup.nextiajd.profiles.numeric.{NumCardinality, NumEntropy, NumIncompleteness, NumUniqueness}
import io.github.haross.nuup.nextiajd.profiles.numeric.patterns.benfords.{Benford1, Benford2, Benford3, Benford4, Benford5, Benford6, Benford7, Benford8, Benford9}
import io.github.haross.nuup.nextiajd.profiles.numeric.value.{AvgVal, MaxValue, MinValue, NumNegatives, NumPositives, SDValue, Variance}
import io.github.haross.nuup.nextiajd.profiles.str.frequency.pct.{MaxPctFrequency, MinPctFrequency, SDPctFrequency}
import io.github.haross.nuup.nextiajd.profiles.str.frequency.{AvgFrequency, Constancy, MaxFrequency, MinFrequency, SDFrequency}
import io.github.haross.nuup.nextiajd.profiles.str.frequency.percentiles.{Octile1, Octile2, Octile3, Octile4, Octile5, Octile6, Octile7}
import io.github.haross.nuup.nextiajd.profiles.str.string.{AvgLength, LongestString, Shortest_string}
import io.github.haross.nuup.nextiajd.profiles.str.words.AvgWordsPerString
import io.github.haross.nuup.nextiajd.profiles.str.{Cardinality, Entropy, Incompleteness, Uniqueness}
import org.apache.jena.vocabulary.{RDF, RDFS, XSD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.types.StringType
import preprocessing.utils.Utils.createDirIfNotExists

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.parallel.ParSeq


// it generates a csv with all true nodes and links created from the ground
object DGL_pre_str_num_nodesAndLinks extends SparkWrapper{

  import spark.implicits._


  def loadSchemaGraphsAndProfiles(parIdAndFileNames: ParSeq[(Int, String)], graphsPath: String, profilesStrPath: String, profilesNumPath: String): ParSeq[(Int, GraphAndProfiles)] = {

    val sourceIDAndGraph = parIdAndFileNames.flatMap { case (id, fileNameNoExt) =>
      val graph = new Graph()
      graph.loadModel(s"$graphsPath/${fileNameNoExt}.ttl")

      val strProfileDF = spark.read.parquet(s"${profilesStrPath}/${fileNameNoExt}")

      // if there is no num profile directory, we assume ds does not have num attributes.
      val path = Paths.get(s"${profilesNumPath}/${fileNameNoExt}")
      if (Files.exists(path)) {

        val numProfileDF = spark.read.parquet(s"${profilesNumPath}/${fileNameNoExt}")
        Some(id -> GraphAndProfiles(graph, strProfileDF, Some(numProfileDF), fileNameNoExt))
      } else {
        Some(id -> GraphAndProfiles(graph, strProfileDF, None, fileNameNoExt))
      }

    }.seq.par

    sourceIDAndGraph

  }

  def getNodesNum(gAndP: GraphAndProfiles, idCounter: AtomicInteger, datasourceId: Int): Seq[NodeNum] = {
    var nodes = Seq.empty[NodeNum]

    val query = s"SELECT ?node ?label ?alias ?domain WHERE { " +
      s" ?node <${RDF.`type`}> <${RDF.Property}> . " +
      s" ?node <${RDFS.label}> ?label . " +
      s" ?node <${RDFS.range}> ?range . " +
      s" ?node <${RDF.value}> ?alias . " +
      s" ?node <${RDFS.domain}> ?domain " +
      s" FILTER NOT EXISTS { ?node <${VocabularyCore.metadata}> <${VocabularyCore.emptyAtt}> .  }" +
      s" FILTER (?range IN (<${XSD.integer}>, <${XSD.xlong}>, <${XSD.xfloat}>, <${XSD.xdouble}>, <${XSD.decimal}>))" +
    s"}"

    val result_nodes = gAndP.graph.runAQuery(query)
    while (result_nodes.hasNext) {
      val r = result_nodes.next()

      val alias = r.get("alias").toString

      nodes = nodes :+ NodeNum(
        id = idCounter.getAndIncrement(),
        iri = r.get("node").toString,
        name = r.get("label").toString,
        dsId = datasourceId,
        dsName = gAndP.fileName,
        // we assume there is a numprofile since graph returns numeric resources
        profile = getNodeNumProfile(alias, gAndP.numProfile.get, gAndP.fileName),
        alias = Some(alias),
        domainURI = Some(r.get("domain").toString))
    }
    nodes
  }



  def getNodesStr(gAndP: GraphAndProfiles, idCounter: AtomicInteger, datasourceId: Int): Seq[NodeStr] = {
    var nodes = Seq.empty[NodeStr]

    val query = s"SELECT ?node ?label ?alias ?domain WHERE { " +
      s" ?node <${RDF.`type`}> <${RDF.Property}> . " +
      s" ?node <${RDFS.label}> ?label . " +
      s" ?node <${RDFS.range}> <${XSD.xstring}> . " +
      s" ?node <${RDF.value}> ?alias . " +
      s" ?node <${RDFS.domain}> ?domain " +
      s" FILTER NOT EXISTS { ?node <${VocabularyCore.metadata}> <${VocabularyCore.emptyAtt}> .  }" + // to exclude empty attrs
      //      s" FILTER NOT EXISTS { ?node <${RDFS.range}> ?range. ?range <${RDF.`type`}> <${RDFS.Class}>  }" +
      s"}"

    val result_nodes = gAndP.graph.runAQuery(query)
    while (result_nodes.hasNext) {
      val r = result_nodes.next()

      val alias = r.get("alias").toString

      nodes = nodes :+ NodeStr(
        id = idCounter.getAndIncrement(),
        iri = r.get("node").toString,
        name = r.get("label").toString,
        dsId = datasourceId  ,
        dsName = gAndP.fileName ,
        profile = getNodeProfile(alias, gAndP.strProfile, gAndP.fileName),
        alias = Some(alias),
        domainURI = Some(r.get("domain").toString))
    }
    nodes
  }

  def getNodeProfile(alias: String, profileDF: DataFrame, fileName: String): Option[ProfilesRow] = {

    val df_tmp = profileDF.filter(col(Commons.attColumn) === alias).select(Commons.profileColumns.map(_.cast(StringType)): _*)
    if (!df_tmp.head(1).isEmpty) {

      val row = df_tmp.first()
      val p = ProfilesRow(
        row.getAs[String](Cardinality.nameAtt), row.getAs[String](Uniqueness.nameAtt), row.getAs[String](Entropy.nameAtt), row.getAs[String](Incompleteness.nameAtt),
        row.getAs[String](AvgLength.nameAtt), row.getAs[String](LongestString.nameAtt), row.getAs[String](Shortest_string.nameAtt),
        row.getAs[String](AvgWordsPerString.nameAtt), row.getAs[String]("cardinality_words_per_col"), row.getAs[String]("max_words_per_string"), row.getAs[String]("min_words_per_string"), row.getAs[String]("sd_words_per_string"),
        row.getAs[String]("very_short_pct"), row.getAs[String]("short_pct"), row.getAs[String]("medium_pct"), row.getAs[String]("long_pct"), row.getAs[String]("very_long_pct"), row.getAs[String]("alphabetic_pct"),
        row.getAs[String]("alphanumeric_pct"), row.getAs[String]("numeric_pct"), /*row.getAs[String]("datetime_pct"),*/ row.getAs[String]("nonAlphanumeric_pct"),
        row.getAs[String](SDFrequency.nameAtt), row.getAs[String](MinFrequency.nameAtt), row.getAs[String](MaxFrequency.nameAtt), row.getAs[String](Constancy.nameAtt), row.getAs[String](AvgFrequency.nameAtt),
        row.getAs[String](Octile1.nameAtt), row.getAs[String](Octile2.nameAtt), row.getAs[String](Octile3.nameAtt), row.getAs[String](Octile4.nameAtt), row.getAs[String](Octile5.nameAtt), row.getAs[String](Octile6.nameAtt), row.getAs[String](Octile7.nameAtt),
        row.getAs[String](MaxPctFrequency.nameAtt), row.getAs[String](MinPctFrequency.nameAtt), row.getAs[String](SDPctFrequency.nameAtt)
      )
      Some(p)
    } else {
      println(s"----> cannot find profile for field ${alias} in dataset ${fileName}. ")
      None
    }

  }

  def getNodeNumProfile(alias: String, numProfileDF: DataFrame, fileName: String): Option[ProfilesNumRow] = {

    val df_tmp = numProfileDF.filter(col(Commons.attColumn) === alias).select(NextiaJD.numericProfilesProjection.map(col).map(_.cast(StringType)): _*)
    if (!df_tmp.head(1).isEmpty) {

      val row = df_tmp.first()

      val p = ProfilesNumRow(
        row.getAs[Any](NumCardinality.nameAtt).toString, row.getAs[Any](NumUniqueness.nameAtt).toString,
        row.getAs[Any](NumEntropy.nameAtt).toString, row.getAs[Any](NumIncompleteness.nameAtt).toString,
        row.getAs[Any](AvgVal.nameAtt).toString, row.getAs[Any](MaxValue.nameAtt).toString,
        row.getAs[Any](MinValue.nameAtt).toString, row.getAs[Any](NumNegatives.nameAtt).toString,
        row.getAs[Any](NumPositives.nameAtt).toString, row.getAs[Any](io.github.haross.nuup.nextiajd.profiles.numeric.value.Range.nameAtt).toString,
        row.getAs[Any](SDValue.nameAtt).toString, row.getAs[Any](Variance.nameAtt).toString,
        row.getAs[Any](Benford1.nameAtt).toString, row.getAs[Any](Benford2.nameAtt).toString,
        row.getAs[Any](Benford3.nameAtt).toString, row.getAs[Any](Benford4.nameAtt).toString,
        row.getAs[Any](Benford5.nameAtt).toString, row.getAs[Any](Benford6.nameAtt).toString,
        row.getAs[Any](Benford7.nameAtt).toString, row.getAs[Any](Benford8.nameAtt).toString,
        row.getAs[Any](Benford9.nameAtt).toString, row.getAs[Any](CoefficientVariation.nameAtt).toString,
        row.getAs[Any](Kurtosis.nameAtt).toString, row.getAs[Any](MAD.nameAtt).toString,
        row.getAs[Any](MedAD.nameAtt).toString, row.getAs[Any](Skewness.nameAtt).toString,
        row.getAs[Any](Quartile1.nameAtt).toString, row.getAs[Any](Quartile2.nameAtt).toString,
        row.getAs[Any](Quartile3.nameAtt).toString, row.getAs[Any](Quartile4.nameAtt).toString,
        row.getAs[Any](IQR.nameAtt).toString
      )

      Some(p)
    } else {
      println(s"----> cannot find profile for field ${alias} in dataset ${fileName}. ")
      None
    }

  }

  def getNodesEntity(gAndP: GraphAndProfiles, idCounter: AtomicInteger, datasourceId: Int): Seq[NodeClass] = {
    var nodes = Seq.empty[NodeClass]
    val query = s"SELECT ?node ?label ?alias WHERE { " +
      s" ?node <${RDF.`type`}> <${RDFS.Class}> .  " +
      s" ?node <${RDFS.label}> ?label . " +
      s" ?node <${RDF.value}> ?alias . " +
      s"}"

    val result_nodes = gAndP.graph.runAQuery(query)

    while (result_nodes.hasNext) {
      val r = result_nodes.next()
      nodes = nodes :+ NodeClass(
        id = idCounter.getAndIncrement(),
        iri = r.get("node").toString,
        name = r.get("label").toString,
        alias = r.get("alias").toString,
        dsId = datasourceId,
        dsName = gAndP.fileName
      )
    }
    nodes
  }


  def loadNodes(graphAndProfile: ParSeq[(Int, GraphAndProfiles)]): (Seq[NodeClass], Seq[NodeStr],  Seq[NodeNum], Map[String, Int]) = {
    // in dgl all node types should start at 0
    val entityCounter = new AtomicInteger(0)
    val strAttCounter = new AtomicInteger(0)
    val numAttCounter = new AtomicInteger(0)

    println("loading entity nodes")
    val parEntityNodes: ParSeq[NodeClass] = graphAndProfile.flatMap { case (id, gAndP) =>
      getNodesEntity(gAndP, entityCounter, id)
    }
    println("loading str nodes")
    val parStrAttNodesNoDomain: ParSeq[NodeStr] = graphAndProfile.flatMap { case (id, gAndP) =>
      getNodesStr(gAndP, strAttCounter, id)
    }
    println("loading num nodes")
    val parNumAttNodesNoDomain: ParSeq[NodeNum] = graphAndProfile.flatMap { case (id, gAndP) =>
      getNodesNum(gAndP, numAttCounter, id)
    }
    println("setting domains")
    val entityIriAndIDs = parEntityNodes.map(node => (node.iri -> node.id)).seq.toMap
    val parStrAttNodes = setDomainIDs(entityIriAndIDs, parStrAttNodesNoDomain).seq
    val parNumAttNodes = setDomainNumIDs(entityIriAndIDs, parNumAttNodesNoDomain).seq


    (parEntityNodes.seq, parStrAttNodes, parNumAttNodes,  entityIriAndIDs)
  }

  def setDomainIDs(iriAndIDs: Map[String, Int], attributeNodes: ParSeq[NodeStr]): ParSeq[NodeStr] = {

    // it's thread-safe since we are modifying each element but their data does not depend on other elements from the same seq
    val attNodes = attributeNodes.map { n =>

      if (n.domainURI.isEmpty) {
        println(s"ERROR: str attribute ${n.name} with iri ${n.iri} does not have a domain. Problem might be when loading nodes")
        n
      } else {
        n.copy(domainID = Some(getIDNode(n.domainURI.get, iriAndIDs)))
      }
    }
    attNodes

  }

  def setDomainNumIDs(iriAndIDs: Map[String, Int], numNodes: ParSeq[NodeNum]): ParSeq[NodeNum] = {

    // it's thread-safe since we are modifying each element but their data does not depend on other elements from the same seq
    val attNodes = numNodes.map { n =>

      if (n.domainURI.isEmpty) {
        println(s"ERROR: num attribute ${n.name} with iri ${n.iri} does not have a domain. Problem might be when loading nodes")
        n
      } else {
        n.copy(domainID = Some(getIDNode(n.domainURI.get, iriAndIDs)))
      }
    }
    attNodes

  }

  def getIDNode(iri: String, nodes: Map[String, Int], defaultValue: Int = -1, nodeType: String = "class"): Int = {

    nodes.get(iri) match {
      case Some(id) =>
        id
      case None =>
        println(s"----> ERROR No ${nodeType} node id found with IRI: $iri----")
        defaultValue
    }

  }

  def loadLinks(graphAndProfile: ParSeq[(Int, GraphAndProfiles)], entityIriAndIDs: Map[String, Int], strAttNodes: Seq[NodeStr], numNodes: Seq[NodeNum]): (ParSeq[Link], ParSeq[Link],ParSeq[Link], Map[String, Int]) = {

    val parRelationshipLinks = graphAndProfile.flatMap { case (id, gAndP) =>
      getObjectPropertiesLinks(gAndP.graph, entityIriAndIDs)
    }
    val strIriAndIDs = strAttNodes.map(node => (node.iri -> node.id)).seq.toMap
    val parStrLinks = graphAndProfile.flatMap { case (id, gAndP) =>
      getLinksStrAtt(gAndP.graph, entityIriAndIDs, strIriAndIDs)
    }
    val numIriAndIDs = numNodes.map(node => (node.iri -> node.id)).seq.toMap
    val parNumLinks = graphAndProfile.flatMap { case (id, gAndP) =>
      getLinksNumAtt(gAndP.graph, entityIriAndIDs, numIriAndIDs)
    }



    (parRelationshipLinks, parStrLinks, parNumLinks, strIriAndIDs)


  }

  def getLinksStrAtt(graph: Graph, entityIriAndIDs: Map[String, Int], strIriAndIDs: Map[String, Int]): Seq[Link] = {

    var links = Seq.empty[Link]
    val query = s"SELECT ?source ?property WHERE { " +
      s" ?property <${RDFS.domain}> ?source. " +
      s" ?property <${RDFS.range}> <${XSD.xstring}>. " +
      s" FILTER NOT EXISTS { ?property <${VocabularyCore.metadata}> <${VocabularyCore.emptyAtt}> . } " +
      s" } "

    val result_nodes = graph.runAQuery(query)
    while (result_nodes.hasNext) {
      val r = result_nodes.next()
      val source = r.get("source").toString
      val target = r.get("property").toString

      val strID = getIDNode(target, strIriAndIDs, Commons.defaultStrDomainID, "str attribute")
      links = links :+ Link(getIDNode(source, entityIriAndIDs), strID, Commons.linkTypes("strAttribute"), "strAttribute")
    }
    links
  }


  def getLinksNumAtt(graph: Graph, entityIriAndIDs: Map[String, Int], numIriAndIDs: Map[String, Int]): Seq[Link] = {

    var links = Seq.empty[Link]
    val query = s"SELECT ?source ?property WHERE { " +
      s" ?property <${RDFS.domain}> ?source. " +
      s" ?property <${RDFS.range}> ?range . " +
      s" FILTER NOT EXISTS { ?property <${VocabularyCore.metadata}> <${VocabularyCore.emptyAtt}> . } " +
      s" FILTER (?range IN (<${XSD.integer}>, <${XSD.xlong}>, <${XSD.xfloat}>, <${XSD.xdouble}>, <${XSD.decimal}>))" +
      s" } "


    val result_nodes = graph.runAQuery(query)
    while (result_nodes.hasNext) {
      val r = result_nodes.next()
      val source = r.get("source").toString
      val target = r.get("property").toString

      val numID = getIDNode(target, numIriAndIDs, Commons.defaultNumDomainID, "num attribute")
      links = links :+ Link(getIDNode(source, entityIriAndIDs), numID, Commons.linkTypes("numAttribute"), "numAttribute")
    }
    links
  }

  def getObjectPropertiesLinks(graph: Graph, entityIriAndIDs: Map[String, Int]): Seq[Link] = {

    var links = Seq.empty[Link]
    val query = s"SELECT ?source ?target WHERE { " +
      s" ?property <${RDFS.domain}> ?source. " +
      s" ?property <${RDFS.range}> ?target. " +
      s" ?target <${RDF.`type`}> <${RDFS.Class}>. " +
      s" } "

    val result_nodes = graph.runAQuery(query)
    while (result_nodes.hasNext) {
      val r = result_nodes.next()
      val source = r.get("source").toString
      val target = r.get("target").toString

      links = links :+ Link(getIDNode(source, entityIriAndIDs), getIDNode(target, entityIriAndIDs), Commons.linkTypes("relationship"), "relationship")
    }
    links
  }


  def createAlignmentsByRepositories(dsInfo: DataFrame, graphAndProfile: ParSeq[(Int, GraphAndProfiles)], entityIriAndIDs: Map[String, Int]): Seq[Link] = {

    // we create alignments aggregating the fileForGraph used. It could also be the collection name
    val parIdAndFileNames = dsInfo.groupBy("repository_alias")
      .agg(
        collect_list(col("id")).as("ids"),
        collect_list(col("filename")).as("filenames")
      ).collect().par.map { row =>
      val repositoryAlias = row.getAs[String]("repository_alias")
      val ids = row.getAs[Seq[Int]]("ids")
      val filenames = row.getAs[Seq[String]]("filenames")
      val files = ids.zip(filenames).map { case (id, file) => IdAndFile(id, file) }


      (repositoryAlias, files)
    }

    val IdAndGraph = graphAndProfile.map(x => x._1 -> x._2.graph).seq.toMap

    val aliEntityLinks = parIdAndFileNames.flatMap { case (repositoryAlias, files) =>
      var alignmentsEntityLinks = Seq.empty[Link]
      println(s"Creating alignments for repository ${repositoryAlias}")

      for ((idAndFileA, index) <- files.zipWithIndex) {
        val graphA = IdAndGraph(idAndFileA.id)
        for (idAndFileB <- files.drop(index + 1)) { // /index starts in 0, but drop starts in 1 so we add +1
//          println(s"alignment for ${idAndFileA.id} - ${idAndFileB.id}")
          val graphB = IdAndGraph(idAndFileB.id)
          alignmentsEntityLinks = alignmentsEntityLinks ++ generateAlignmentsEntity(graphA, graphB, entityIriAndIDs, idAndFileA.file, idAndFileB.file)
        }
      }
      alignmentsEntityLinks
    }.seq

    println(s"size alignments: ${aliEntityLinks.size}")
    aliEntityLinks

  }


  def generateAlignmentsEntity(graphA: Graph, graphB: Graph, entityIriAndIDs: Map[String, Int], fileNameA: String, fileNameB: String): Seq[Link] = {
    var links = Seq.empty[Link]
    // TODO: maybe alignments need to check class has some string attributes
    val query = s"SELECT DISTINCT ?class ?alias WHERE { " +
      s" ?class <${RDF.`type`}> <${RDFS.Class}> . " +
      s" ?class <${RDF.value}> ?alias .  " +
      //      s" UNION { " +
      //      s" ?node <${RDF.`type`}> <${RDF.Property}>." +
      //      s" ?node <${RDFS.range}> <${XSD.xstring}> . } " +
      s" }"

    val result_nodesA = graphA.runAQuery(query)
    val result_nodesB = graphB.runAQuery(query)
    val mapA = scala.collection.mutable.Map[String, String]()

    while (result_nodesA.hasNext) {
      val r = result_nodesA.next()
      val classNode = r.getResource("class").getURI
      val alias = r.getLiteral("alias").toString

      mapA += (alias -> classNode)
    }

    while (result_nodesB.hasNext) {

      val r = result_nodesB.next()
      val classNodeB = r.getResource("class").getURI
      val aliasB = r.getLiteral("alias").toString

      if (mapA.contains(aliasB)) {
        val classNodeA = mapA(aliasB)
        links = links :+ Link(getIDNode(classNodeA, entityIriAndIDs), getIDNode(classNodeB, entityIriAndIDs), Commons.linkTypes("entityAlignment"), "entityAlignment")
        //        println(s"alignment for alias ${aliasB}")
      } else {
        if (aliasB.equals(fileNameB)) {
          //          links = links :+ Link(getIDNode(classNodeA,entityIriAndIDs), getIDNode(classNodeB,entityIriAndIDs), linkTypes("entityAlignment"), "entityAlignment")

          val classNodeA = mapA(fileNameA)
          links = links :+ Link(getIDNode(classNodeA, entityIriAndIDs), getIDNode(classNodeB, entityIriAndIDs), Commons.linkTypes("entityAlignment"), "entityAlignment")
          //          println(s"alignment root ${classNodeA} and ${classNodeB}")
        } else {
          println(s"*--->  cannot find alignment for alias: $aliasB")
        }

      }

    }
    links
  }


  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/data"
    val graphsWithMetadataPath = s"${basePath}/graphsWithMetadata" // contains empty property metadata
    val profilePathNormByDS = s"${basePath}/profiles_normStrByDS"
    val numProfilePathNormByDS = s"${basePath}/profiles_normNumByDS"
    val outputFolder = s"${basePath}/pre_nodes_links"

    createDirIfNotExists(outputFolder)

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"${basePath}/datasetInfo.csv")
//      .filter(col("active") === true)
//      .filter(col("testing2") === false)

    val parIdAndFileNames = dsInfo.select("id", "fileName").as[(Int, String)].collect().toList.par

    println("Loading schema graphs and profiles")
    val gAndProfiles = loadSchemaGraphsAndProfiles(parIdAndFileNames, graphsWithMetadataPath, profilePathNormByDS, numProfilePathNormByDS)
    println("Loading nodes")
    val (entityNodes, strAttNodes, numNodes, entityIriAndIDs) = loadNodes(gAndProfiles)

    println("entity nodes" ,entityNodes.size)
    println("str nodes", strAttNodes.size)
    println("num nodes", numNodes.size)

    println("Loading links")
    val (relationshipLinks, attrLinks, numLinks, strIriAndIDs) = loadLinks(gAndProfiles, entityIriAndIDs, strAttNodes, numNodes)

    println("Creating alignments by repositories")
    val alignmentsLinks = createAlignmentsByRepositories(dsInfo, gAndProfiles, entityIriAndIDs)


    writeNodeFiles(entityNodes, strAttNodes, numNodes , outputFolder)
    writeLinksFile(relationshipLinks.seq, outputFolder, "rel_links.csv")
    writeLinksFile(attrLinks.seq, outputFolder, "str_links.csv")
    writeLinksFile(numLinks.seq, outputFolder, "num_links.csv")
    writeLinksFile( alignmentsLinks , outputFolder, "alignments.csv")

  }

}
