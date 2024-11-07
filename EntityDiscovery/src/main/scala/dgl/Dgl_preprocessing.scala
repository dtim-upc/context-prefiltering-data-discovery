package dgl


import core.SparkWrapper
import edu.upc.essi.dtim.nextiadi.jena.Graph
import io.github.haross.nuup.nextiajd.NextiaJD
import io.github.haross.nuup.nextiajd.profiles.str.Cardinality
import org.apache.jena.vocabulary.{RDF, RDFS, XSD}
import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import preprocessing.utils.Utils.{createDirIfNotExists, writeFile}

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

object Dgl_preprocessing extends SparkWrapper {


  val profileColumns = NextiaJD.strProfilesProjection.map(col)
  val attColumn = "colName"
  val dsColumn = "dataset"

  val nodeTypes = Map("entity" -> "0", "attribute" -> "1")
  val linkTypes = Map("strAttribute" -> "1","relationship" -> "2", "entityAlignment" -> "0")

  case class NodeClass( id:Int, iri:String, name:String, alias:String )
  case class NodeStr ( id:Int, iri:String, name:String, var profile:Option[ProfilesRow], var alias:Option[String], var domainID:Option[Int] = None, var domainURI:Option[String] = None )
  case class Link(src_id:Int, target_id:Int, linkType:String, typeStr:String, linkWeight:String = "1")
  case class ResultRow(id: Int, filename: String, repository:String)
  case class ProfilesRow(cardinalityRaw:String,
                         cardinality: String, uniqueness: String, entropy: String, incompleteness: String,
                         avg_length_string: String, longest_string: String, shortest_String: String,
                         avg_words_per_value: String, cardinality_words_per_col: String, max_words_per_string: String, min_words_per_string:String, sd_words_per_string: String,
                         very_short_pct: String, short_pct: String, medium_pct: String, long_pct: String, very_long_pct: String, alphabetic_pct: String,
                         alphanumeric_pct: String, numeric_pct: String, datetime_pct: String, nonAlphanumeric_pct: String,
                         sd_frequency: String, min_frequency: String, max_frequency: String, constancy: String, avg_frequency: String,
                         octile_1: String, octile_2:String, octile_3: String, octile_4: String, octile_5:String, octile_6: String, octile_7: String,
                         max_pct_frequency: String, min_pct_frequency: String, sd_pct_frequency: String
                        ) {
    def mkStringP: String = List(
      cardinality, uniqueness, entropy, incompleteness,
      avg_length_string, longest_string, shortest_String,
      avg_words_per_value, cardinality_words_per_col, max_words_per_string, min_words_per_string, sd_words_per_string,
      very_short_pct, short_pct, medium_pct, long_pct, very_long_pct, alphabetic_pct,
      alphanumeric_pct, numeric_pct, datetime_pct, nonAlphanumeric_pct,
      sd_frequency, min_frequency, max_frequency, constancy, avg_frequency,
      octile_1, octile_2, octile_3, octile_4, octile_5, octile_6, octile_7,
      max_pct_frequency, min_pct_frequency, sd_pct_frequency
    ).mkString(",")
  }


  def getNodesEntity(graph:Graph, idCounter: AtomicInteger): Seq[NodeClass] ={
    var nodes = Seq.empty[NodeClass]
    val query = s"SELECT ?node ?label ?alias WHERE { " +
      s" ?node <${RDF.`type`}> <${RDFS.Class}> .  " +
      s" ?node <${RDFS.label}> ?label . " +
      s" ?node <${RDF.value}> ?alias . " +
      s"}"

    val result_nodes = graph.runAQuery(query)

    val defaultProfile = Seq.fill(profileColumns.length){1}.mkString(",")

    while(result_nodes.hasNext){
      val r = result_nodes.next()
      nodes = nodes :+ NodeClass(
        id = idCounter.getAndIncrement() ,
        iri = r.get("node").toString,
        name = r.get("label").toString,
        alias = r.get("alias").toString )
    }
    nodes
  }

  def getNodesStr(graph:Graph, idCounter: AtomicInteger): Seq[NodeStr] ={
    var nodes = Seq.empty[NodeStr]
    val query = s"SELECT ?node ?label ?alias ?domain WHERE { " +
      s" ?node <${RDF.`type`}> <${RDF.Property}> . " +
      s" ?node <${RDFS.label}> ?label . " +
      s" ?node <${RDFS.range}> <${XSD.xstring}> . " +
      s" ?node <${RDF.value}> ?alias . " +
      s" ?node <${RDFS.domain}> ?domain " +
      //      s" FILTER NOT EXISTS { ?node <${RDFS.range}> ?range. ?range <${RDF.`type`}> <${RDFS.Class}>  }" +
      s"}"

    //    val defaultProfile = Seq.fill(profileColumns.length){1}.mkString(",")
    val result_nodes = graph.runAQuery(query)
    while(result_nodes.hasNext){
      val r = result_nodes.next()
      nodes = nodes :+ NodeStr(
        id = idCounter.getAndIncrement(),
        iri = r.get("node").toString,
        name = r.get("label").toString,
        profile = None,
        alias = Some(r.get("alias").toString),
        domainURI = Some(r.get("domain").toString) )
    }
    nodes
  }

  def getIDClass(iri:String, nodes:Seq[NodeClass]): Int = {

    nodes.find(_.iri == iri) match {
      case Some(node) =>
        node.id
      case None =>
        println(s"----No class node id found with IRI: $iri----")
        -1
    }

  }

  def getID(iri:String, nodes:Seq[NodeStr]): Int = {

    nodes.find(_.iri == iri) match {
      case Some(node) =>
        node.id
      case None =>
        println(s"----No node str id found with IRI: $iri----")
        -2
    }

  }

  def setDomainIDs(entityNodes:Seq[NodeClass], attributeNodes:Seq[NodeStr]): Unit ={

    attributeNodes.foreach{ n =>
      n.domainURI match {
        case Some(domainURI) =>
          // domains are always entities
          n.domainID = Some( getIDClass(domainURI, entityNodes) )
        case None => //do nothing
      }
    }

  }

  def getObjectPropertiesLinks(graph:Graph, entityNodes:Seq[NodeClass]): Seq[Link] ={

    var links = Seq.empty[Link]
    val query = s"SELECT ?source ?target WHERE { " +
      s" ?property <${RDFS.domain}> ?source. " +
      s" ?property <${RDFS.range}> ?target. " +
      s" ?target <${RDF.`type`}> <${RDFS.Class}>. " +
      s" } "

    val result_nodes = graph.runAQuery(query)
    while( result_nodes.hasNext ){
      val r = result_nodes.next()
      val source = r.get("source").toString
      val target = r.get("target").toString

      links = links :+ Link(getIDClass(source,entityNodes), getIDClass(target,entityNodes),  linkTypes.get("relationship").get, "relationship"  )
    }
    links
  }

  def getLinksStrAtt(graph:Graph, entityNodes:Seq[NodeClass] ,attrNodes:Seq[NodeStr]): Seq[Link] ={

    var links = Seq.empty[Link]
    val query = s"SELECT ?source ?property WHERE { " +
      s" ?property <${RDFS.domain}> ?source. " +
      s" ?property <${RDFS.range}> <${XSD.xstring}>. " +
      //      s" FILTER NOT EXISTS { ?target <${RDF.`type`}> <${RDFS.Class}>  }" +
      s" } "

    val result_nodes = graph.runAQuery(query)
    while( result_nodes.hasNext ){
      val r = result_nodes.next()
      val source = r.get("source").toString
      val target = r.get("property").toString

      val strID = getID(target,attrNodes)
      if(strID != -2) // if it's -2, id could not be found and most probably is an empty str with cardinality 0
        links = links :+ Link(getIDClass(source,entityNodes),strID , linkTypes.get("strAttribute").get, "strAttribute")
    }
    links
  }

  def generateAlignmentsEntity(graphA:Graph,graphB:Graph,  entityNodes:Seq[NodeClass], fileNameA: String, fileNameB:String): Seq[Link] = {
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

    while( result_nodesA.hasNext ){
      val r = result_nodesA.next()
      val classNode =r.getResource("class").getURI
      val alias = r.getLiteral("alias").toString

      mapA += (alias -> classNode)
    }

    while( result_nodesB.hasNext ){

      val r = result_nodesB.next()
      val classNodeB =r.getResource("class").getURI
      val aliasB = r.getLiteral("alias").toString

      if(mapA.contains(aliasB)){
        val classNodeA = mapA(aliasB)
        links = links :+ Link(getIDClass(classNodeA,entityNodes), getIDClass(classNodeB,entityNodes), linkTypes.get("entityAlignment").get, "entityAlignment")
        //        println(s"alignment for alias ${aliasB}")
      } else {
        if(aliasB.equals(fileNameB) ){
//          links = links :+ Link(getIDClass(classNodeA,entityNodes), getIDClass(classNodeB,entityNodes), linkTypes.get("entityAlignment").get, "entityAlignment")

          val classNodeA = mapA(fileNameA)
          links = links :+ Link(getIDClass(classNodeA,entityNodes), getIDClass(classNodeB,entityNodes), linkTypes.get("entityAlignment").get, "entityAlignment")
//          println(s"alignment root ${classNodeA} and ${classNodeB}")
        } else {
          println(s"*--->  cannot find alignment for alias: $aliasB")
        }

      }

    }
    links
  }


  // for a positive pair (e1, e2), sample (e1, e') as a negative pair
  // where e' associates with e1 via Entity-Relationship-Entity metapath (i.e., a n-hop neighbor)
  // and (e1, e') does not exist in the original graph
  // We don't check the metapath since bootstrapping connects all classes with relationships. So all classes from the schema are connected
  // create a negative link between resources of the same bootstrapping graph
  def generateNegativeRelationalSampling(graph: Graph, entityNodes:Seq[NodeClass]): Seq[Link] ={
    var links = Seq.empty[Link]
    val query = s"SELECT ?node WHERE { " +
      s" ?node <${RDF.`type`}> <${RDFS.Class}>  " +
      s" }"

    val result_nodes = graph.runAQuery(query)
    var entities = Seq.empty[String]
    while( result_nodes.hasNext ){
      val r = result_nodes.next()
      entities = entities :+ r.get("node").toString
    }

    for((e1,i) <- entities.zipWithIndex){
      for(e_prime <- entities.drop(i+1)){ // remove e1 from entities list
        links = links :+ Link(getIDClass(e1,entityNodes), getIDClass(e_prime,entityNodes), linkTypes("entityAlignment"), "NegativeEntityAlignment")
        // for safety I'm adding the inverse...
        links = links :+ Link(getIDClass(e_prime,entityNodes), getIDClass(e1,entityNodes), linkTypes("entityAlignment"), "NegativeEntityAlignment")
      }
    }
    links
  }

  // this one are true negatives, but we want to keep a formal procedure. I think this one would be typed sampling
  @Deprecated
  def generateNegativeAlignmentsEntity(graph:Graph, entityNodes:Seq[NodeClass], idA: Int, idB:Int): Seq[Link] = {
    var links = Seq.empty[Link]
    val query = s"SELECT ?node WHERE { " +
      s" ?node <${RDF.`type`}> <${RDFS.Class}>  " +
      s" }"

    val result_nodes = graph.runAQuery(query)
    var resources = Seq.empty[String]
    while( result_nodes.hasNext ){
      val r = result_nodes.next()
      val sourceNamespace =r.getResource("node").getNameSpace
      val source = r.get("node").toString.replace(sourceNamespace, s"http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/${idA}/")
      resources = resources :+ source
    }

    for((r1,i) <- resources.zipWithIndex){

      for(r2 <- resources.drop(i+1)){

        // create a negative link between resources of the same bootstrapping graph
        links = links :+ Link(getIDClass(r1,entityNodes), getIDClass(r2,entityNodes), linkTypes.get("entityAlignment").get, "NegativeEntityAlignment")
        val target = r2.replace(s"http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/${idA}/", s"http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/${idB}/")
        // create negative link between resources of g1 vs g2
        links = links :+ Link(getIDClass(r1,entityNodes), getIDClass(target,entityNodes), linkTypes.get("entityAlignment").get, "NegativeEntityAlignment")

        //        println("***" + r1.replace("http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/","") + " ---> " + r2.replace("http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/","") )
        //        println("*** ->" + r1.replace("http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/","") + " ---> " + target.replace("http://www.essi.upc.edu/DTIM/NextiaDI/DataSource/Schema/","") )

      }
    }
    links
  }

  def getProfileFor(df: DataFrame, nodeAlias: String): String ={

    var alias = nodeAlias.replace(".","_")
    //this is for method.mash_temp_temp.unit since it is mash_temp_temp.unit but graph has method.mash_temp_temp.unit. BeersAPI_1.profile.parquet)
    if(nodeAlias.contains("method.mash_temp_temp.")){
      alias = alias.replace("method_","")
    } else if(nodeAlias.contains("ingredients.hops")){
      alias = alias.replace("ingredients_hops","hops")
    } else if(nodeAlias.contains("ingredients.malt")){
      alias = alias.replace("ingredients_malt","malt")
    }

    val nestedAlias = if (nodeAlias.contains("_"))  nodeAlias.split("_", 2)(1) else "NoNestedAlias"
    val nestedAlias2 = nodeAlias.replace("_view","")
    val nestedAlias3 = alias.replace("_view","")


    val valuesSeq = Seq(alias, nestedAlias, nestedAlias2 ,nestedAlias3)

    val matchingValues = df.filter(col(attColumn).isin(valuesSeq:_*)).select(attColumn).distinct().collect().map(_.getString(0)).toSeq

    //    println("**")
    //    println(matchingValues)
    //    println(valuesSeq)
    //    println("head: "+matchingValues.head)
    matchingValues.head
  }

  def setNodesProfiles(nodesStr: Seq[NodeStr], profilePath: String, profilePathRaw: String, fileName: String ): Unit ={

    println(s"reading profile ${profilePath}/${fileName}")
    val profilesRaw = spark.read.parquet(s"${profilePathRaw}/${fileName}")
    val profilesDS_norm = spark.read.parquet(s"${profilePath}/${fileName}")

    println(s"${fileName} has columns: ${profilesDS_norm.count()}")

    for (node <- nodesStr) {

      val df_tmp = profilesDS_norm.filter(col(attColumn) === node.alias.get).select(profileColumns.map(_.cast(StringType)):_*)
      val cardRaw = profilesRaw.filter(col(attColumn) === node.alias.get).select(col(Cardinality.nameAtt).cast(StringType)).first().getString(0)

      if(!df_tmp.head(1).isEmpty) {

        val row = df_tmp.first()
        val p = ProfilesRow(cardRaw,
          row.getAs[String]("cardinality"), row.getAs[String]("uniqueness"), row.getAs[String]("entropy"), row.getAs[String]("incompleteness"),
          row.getAs[String]("avg_length_string"), row.getAs[String]("longest_string"), row.getAs[String]("shortest_String"),
          row.getAs[String]("avg_words_per_value"), row.getAs[String]("cardinality_words_per_col"), row.getAs[String]("max_words_per_string"), row.getAs[String]("min_words_per_string"), row.getAs[String]("sd_words_per_string"),
          row.getAs[String]("very_short_pct"), row.getAs[String]("short_pct"), row.getAs[String]("medium_pct"), row.getAs[String]("long_pct"), row.getAs[String]("very_long_pct"), row.getAs[String]("alphabetic_pct"),
          row.getAs[String]("alphanumeric_pct"), row.getAs[String]("numeric_pct"), row.getAs[String]("datetime_pct"), row.getAs[String]("nonAlphanumeric_pct"),
          row.getAs[String]("sd_frequency"), row.getAs[String]("min_frequency"), row.getAs[String]("max_frequency"), row.getAs[String]("constancy"), row.getAs[String]("avg_frequency"),
          row.getAs[String]("octile_1"), row.getAs[String]("octile_2"), row.getAs[String]("octile_3"), row.getAs[String]("octile_4"), row.getAs[String]("octile_5"), row.getAs[String]("octile_6"), row.getAs[String]("octile_7"),
          row.getAs[String]("max_pct_frequency"), row.getAs[String]("min_pct_frequency"), row.getAs[String]("sd_pct_frequency")
        )
        node.profile = Some(p)
      } else {
        println(s"----> cannot find profile for field ${node.alias.get} in dataset ${fileName}. ")
      }

    }

  }

  //  def writeNodesFiles(graph:Graph, outputFolder: String): Unit ={
  //    val nodes = getNodesEntity(graph) ++  getNodesStr(graph)
  //    val nodesIndex = nodes.zipWithIndex
  //
  //    var content = "node_id,node_name,node_type,node_iri,node_attributes\n"
  //    nodesIndex.foreach( {
  //      case(node, id) => content = content + Seq(id,node.name,node.node_type,node.iri,node.profile).mkString(",") + "\n"
  //    } )
  //
  //    println(content)
  //    writeFile(content, outputFolder, "nodes.csv")
  //
  //    val links = getObjectPropertiesLinks(graph, nodesIndex ) ++ getLinksStrAtt(graph, nodesIndex)
  //    var linkContent = "src_id,dst_id,link_type,link_weight"
  //    links.foreach(l => linkContent = linkContent + Seq(l.src_id,l.target_id,l.linkType,l.linkWeight).mkString(",") + "\n")
  //
  //    writeFile(linkContent, outputFolder,"links.csv" )
  //
  //  }


  def loadSchemaGraphs(dsInfo: DataFrame, graphsPath: String): Map[Int,Graph] ={

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

  def loadNodes(dsInfo: DataFrame, profilePathNorm: String, profilePathRaw: String, graphDict: Map[Int,Graph]): (Seq[NodeClass], Seq[NodeStr], Seq[NodeStr]) ={

    var entityNodes = Seq.empty[NodeClass]
    var strAttNodes = Seq.empty[NodeStr]

    // in dgl all node types should start at 0
    val entityCounter = new AtomicInteger(0)
    val strAttCounter = new AtomicInteger(0)

    // we read entities and string attributes nodes. Additionally, we set up profiles
    dsInfo.select("id","fileName").collect().foreach{
      case Row(id: Int, fileName: String) =>

        val eNodes = getNodesEntity(graphDict(id), entityCounter)
        val sNodes = getNodesStr(graphDict(id), strAttCounter)

        setNodesProfiles(sNodes, profilePathNorm, profilePathRaw  , fileName )

        entityNodes = entityNodes ++ eNodes
        strAttNodes = strAttNodes ++ sNodes
    }

    // It modifies strAttNodes by reference
    setDomainIDs(entityNodes, strAttNodes)

    val strAttNodes_empty =  strAttNodes.filter(_.profile.get.cardinalityRaw.equals("0"))
    val strAttNodesFinal = strAttNodes.diff(strAttNodes_empty)

    var currentId = 0

    val strWithNewIds: Seq[NodeStr] = strAttNodesFinal.map { node =>
      val newNode = node.copy(id = currentId)
      currentId += 1
      newNode
    }

    println(s"nodes empty ${strAttNodes_empty.size} and nodes: ${strAttNodes.size}")

    (entityNodes, strWithNewIds, strAttNodes_empty)
  }

  def loadLinks(dsInfo: DataFrame, graphs: Map[Int,Graph], entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStr]): (Seq[Link], Seq[Link] )={

    var relationshipLinks = Seq.empty[Link]
    var attrLinks = Seq.empty[Link]
    dsInfo.select("id","fileName").collect().foreach{
      case Row(id: Int, fileName: String) =>
        val graph = graphs(id)
        relationshipLinks = relationshipLinks ++ getObjectPropertiesLinks(graph, entityNodes )
        attrLinks = attrLinks ++ getLinksStrAtt(graph, entityNodes, strAttNodes)
    }
    (relationshipLinks, attrLinks)

  }

  def createNegativeAlignments(dsInfo: DataFrame, graphs: Map[Int,Graph], entityNodes: Seq[NodeClass]): Seq[Link] ={
    var negativeLinks = Seq.empty[Link]

    dsInfo.select("id","fileName").collect().foreach{
      case Row(id: Int, fileName: String) =>
        val graph = graphs(id)
        negativeLinks = negativeLinks ++  generateNegativeRelationalSampling(graph, entityNodes)
    }

    negativeLinks
  }

  def createAlignmentsByRepositories(dsInfo: DataFrame, graphs: Map[Int,Graph], entityNodes: Seq[NodeClass]): Seq[Link] ={

    var alignmentsEntityLinks = Seq.empty[Link]


    // we create alignments aggregating the fileForGraph used. It could also be the collection name
    dsInfo.withColumn("id_filename", struct(col("id"), col("filename"), col("repository_alias") ))
      .groupBy("repository_alias").agg(collect_list("id_filename").as("files"))
      .collect().foreach( r => {
      val ids_files = r.getAs[Seq[Row]]("files").map(x => ResultRow(x(0).toString.toInt,x(1).toString, x(2).toString))


      for ((v, i) <- ids_files.zipWithIndex) {
        val graphA = graphs(v.id)
        for( k <- ids_files.drop(i+1)) { //index starts in 0, but drop starts in 1 so we add +1
          println(s"alignment for ${v.id} - ${k.id}")
          val graphB = graphs(k.id)
          alignmentsEntityLinks = alignmentsEntityLinks ++ generateAlignmentsEntity(graphA,graphB, entityNodes, v.filename, k.filename )
        }
      }
    })

    alignmentsEntityLinks

  }


  def writeLinksFile(links: Seq[Link], outputFolder:String, fileName:String): Unit ={

    var linkContent = "src_id,dst_id,link_type,str_type,link_weight\n"
    links.foreach(l => linkContent = linkContent + Seq(l.src_id,l.target_id,l.linkType,l.typeStr,l.linkWeight).mkString(",") + "\n")
    writeFile(linkContent, outputFolder,fileName )

  }

  def writeInverseLinksFile(links: Seq[Link], outputFolder:String, fileName:String): Unit ={

    var linkContent = "src_id,dst_id,link_type,str_type,link_weight\n"
    links.foreach(l => linkContent = linkContent + Seq(l.target_id,l.src_id,l.linkType,l.typeStr,l.linkWeight).mkString(",") + "\n")
    writeFile(linkContent, outputFolder,fileName )

  }



  def writeNodeFiles(entityNodes:Seq[NodeClass], strAttNodes:Seq[NodeStr], outputFolder:String ): Unit ={
    var contentEntity = "node_id,node_name,node_iri,node_attributes\n"
    entityNodes.foreach{ node =>
      //         println("***" + node.profile.getOrElse("NOT FOUND") )
      contentEntity = contentEntity + Seq(node.id,node.name,node.iri,"\""+Seq.fill(profileColumns.length){1}.mkString(",") ).mkString(",")+"\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,domain_id,node_iri,node_attributes\n"
    strAttNodes.foreach{ node =>
      contentAttr = contentAttr + Seq(node.id,node.name,node.domainID.getOrElse("NO_DOMAIN"),node.iri,"\""+node.profile.get.mkStringP).mkString(",")+"\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "attributeNodes.csv")
  }

  def splitAlignmentsLinks(alignmentsLinks: Seq[Link]): (Seq[Link],Seq[Link],Seq[Link]) ={
    // sample alignments links
    val seed = 1023
    val random = new Random(seed)
    // we will take 30%: 15% for validation and 15% for testing
    val percent = (alignmentsLinks.size * 0.3).toInt
    val sampleValAndTest = random.shuffle(alignmentsLinks).take(percent)
    val percentTest = (sampleValAndTest.size * 0.5).toInt
    val alignmentsTest =  sampleValAndTest.take(percentTest)
    val alignmentsVal = sampleValAndTest.diff(alignmentsTest)
    val alignmentsTraining = alignmentsLinks.diff(sampleValAndTest)

    println(s"initial size: ${alignmentsLinks.size}")
    println(s"test size: ${alignmentsTest.size}")
    println(s"val size: ${alignmentsVal.size}")
    println(s"train size: ${alignmentsTraining.size}")

    (alignmentsTraining, alignmentsVal, alignmentsTest)
  }


  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/data"
    val graphsWithMetadataPath = s"${basePath}/graphsWithMetadata" // contains empty property metadata
    val profilesPath =  s"${basePath}/profiles"
//    val profilesNorm = s"${basePath}/profiles_norm"
    val profilePathNormByDS = s"${basePath}/profiles_normByDS"
    val outputFolder = s"${basePath}/input_dgl"

    createDirIfNotExists(outputFolder)

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"${basePath}/datasetInfo.csv")
      .filter(col("active") === true)
      .filter(col("testing2") === false)
      .limit(50) // those are for another exp

    val sourceGraphs = loadSchemaGraphs(dsInfo, graphsWithMetadataPath)


    val (entityNodes, strAttNodes, _) = loadNodes(dsInfo, profilePathNormByDS, profilesPath , sourceGraphs)

    val (relationshipLinks, attrLinks) = loadLinks(dsInfo,sourceGraphs, entityNodes, strAttNodes)
    val alignmentsLinks = createAlignmentsByRepositories(dsInfo,sourceGraphs, entityNodes)
    val (alignmentsTraining, alignmentsVal, alignmentsTest) = splitAlignmentsLinks(alignmentsLinks)

    // we sort the alignments by src_id to get the positions on the adjacency matrix.
    val alignmentsAdjM = alignmentsLinks.sortBy(_.src_id)
    val train_idx = alignmentsTraining.map(link => alignmentsAdjM.indexOf(link) ).sorted
    val val_idx = alignmentsVal.map(link => alignmentsAdjM.indexOf(link) ).sorted
    val test_idx =  alignmentsTest.map(link => alignmentsAdjM.indexOf(link) ).sorted
    //    println(train_idx)
    //    println(val_idx)
    //    println(test_idx)

    var negativeLinks = createNegativeAlignments(dsInfo, sourceGraphs,entityNodes)

    var val_neg_edges = Seq.empty[(Int, Int)]
    // create negatives for validation
    val uniqueSrcIdsWithCount = alignmentsVal.groupBy(_.src_id).mapValues(_.size).toList
    for( (e1, count) <- uniqueSrcIdsWithCount){
      for( i <- 1 to count ){
        val linkToRemove = negativeLinks.find(_.src_id == e1)
        negativeLinks = linkToRemove match {
          case Some(link) => negativeLinks.filterNot(_ == link)
          case None =>
            println(s"----> error creating negative links!!!! ${e1} and count ${count}")
//            alignmentsVal.filter()
            println(negativeLinks.map(_.src_id).toSeq)
            println(negativeLinks.map(_.target_id).toSeq)
            negativeLinks
        }
         if(!linkToRemove.isEmpty)
            val_neg_edges = val_neg_edges :+ (e1, linkToRemove.get.target_id)

      }
    }
    val_neg_edges = val_neg_edges.sortBy(_._1)
    // obtain negatives for test
    val seed = 1023
    val random = new Random(seed)
    val test_neg_edges = random.shuffle(negativeLinks).take(alignmentsTest.size).map(x => (x.src_id, x.target_id)).sortBy(_._1)


    writeNodeFiles(entityNodes, strAttNodes, outputFolder)
    writeLinksFile(relationshipLinks, outputFolder, "g_rel_links.csv")
    writeLinksFile(attrLinks, outputFolder, "g_attr_links.csv")
    writeInverseLinksFile(attrLinks.sortBy(_.target_id), outputFolder, "g_attr_inv_links.csv")
    // not sure if I should order links. for now leave it
    writeLinksFile(alignmentsTraining, outputFolder, "g_train_alignments_links.csv")
    writeLinksFile( alignmentsTraining ++ alignmentsVal , outputFolder, "g_val_alignments_links.csv")
    writeLinksFile( alignmentsTraining ++ alignmentsVal ++ alignmentsTest , outputFolder, "g_test_alignments_links.csv")

    var helperContent = "key,value\n"

    //    Seq(node.id,node.name,node.iri,"\""+node.profile.getOrElse("NOT FOUND")).mkString(",")+"\"" + "\n"

    helperContent = helperContent + "test_neg_edges,\""+test_neg_edges.map{ case (x, y) => s"$x,$y" }.mkString(";")+"\"\n"
    helperContent = helperContent + "val_neg_edges,\""+val_neg_edges.map{ case (x, y) => s"$x,$y" }.mkString(";")+"\"\n"
    helperContent = helperContent + "train_idx,\""+train_idx.mkString(",")+"\"\n"
    helperContent = helperContent + "val_idx,\""+val_idx.mkString(",")+"\"\n"
    helperContent = helperContent + "test_idx,\""+test_idx.mkString(",")+"\"\n"

    writeFile(helperContent, outputFolder, "helpers.csv" )

  }

}

