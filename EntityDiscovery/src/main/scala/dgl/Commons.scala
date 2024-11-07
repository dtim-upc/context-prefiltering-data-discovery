package dgl

import io.github.haross.nuup.nextiajd.NextiaJD
import org.apache.spark.sql.functions.col
import preprocessing.utils.Utils.writeFile

object Commons {
  val profileColumns = NextiaJD.strProfilesProjection.map(col)
  val nodeTypes = Map("entity" -> "0", "attribute" -> "1")
  val linkTypes = Map("boolAttribute"->"4","numAttribute" -> "3", "strAttribute" -> "1", "relationship" -> "2", "entityAlignment" -> "0")
  val attColumn = "colName"
  val dsColumn = "dataset"

  val defaultStrDomainID = -2
  val defaultNumDomainID = -3
  val defaultBoolDomainID = -4


  def writeNodeFiles(entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStr], outputFolder: String): Unit = {

    var contentEntity = "node_id,node_name,alias,node_iri,ds_id,ds_name,node_attributes\n"
    entityNodes.foreach { node =>

      contentEntity = contentEntity + Seq(node.id, node.name, node.alias, node.iri, node.dsId, node.dsName, "\"" +
        Seq.fill(profileColumns.length) {
          1
        }.mkString(",")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    strAttNodes.foreach { node =>
      contentAttr = contentAttr + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.get.mkStringP).mkString(",") + "\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "strNodes.csv")
  }

  def writeNodeFiles(entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStr], numNodes: Seq[NodeNum], outputFolder: String): Unit = {

    var contentEntity = "node_id,node_name,alias,node_iri,ds_id,ds_name,node_attributes\n"
    entityNodes.foreach { node =>

      contentEntity = contentEntity + Seq(node.id, node.name, node.alias, node.iri, node.dsId, node.dsName, "\"" +
        Seq.fill(profileColumns.length) {
          1
        }.mkString(",")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    strAttNodes.foreach { node =>
      contentAttr = contentAttr + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.get.mkStringP.replace("NaN", "0.0")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "strNodes.csv")


    var contentNum = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    numNodes.foreach { node =>
      contentNum = contentNum + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.get.mkStringP.replace("NaN", "0.0")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentNum, outputFolder, "numNodes.csv")

  }

  def writeNodeFilesV2(entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStrV2], numNodes: Seq[NodeNumV2], outputFolder: String): Unit = {

    var contentEntity = "node_id,node_name,alias,node_iri,ds_id,ds_name,node_attributes\n"
    entityNodes.foreach { node =>

      contentEntity = contentEntity + Seq(node.id, node.name, node.alias, node.iri, node.dsId, node.dsName, "\"" +
        Seq.fill(profileColumns.length) {
          1
        }.mkString(",")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    strAttNodes.foreach { node =>
      contentAttr = contentAttr + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.replace("NaN", "0.0")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "strNodes.csv")


    var contentNum = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    numNodes.foreach { node =>
      contentNum = contentNum + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.replace("NaN", "0.0")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentNum, outputFolder, "numNodes.csv")

  }

  def writeNodeFiles(entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStr], numNodes: Seq[NodeNum],boolNodes: Seq[NodeBool], outputFolder: String): Unit = {

    var contentEntity = "node_id,node_name,alias,node_iri,ds_id,ds_name,node_attributes\n"
    entityNodes.foreach { node =>

      contentEntity = contentEntity + Seq(node.id, node.name, node.alias, node.iri, node.dsId, node.dsName, "\"" +
        Seq.fill(profileColumns.length) {
          1
        }.mkString(",")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    strAttNodes.foreach { node =>
      contentAttr = contentAttr + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.get.mkStringP.replace("NaN", "0.0")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "strNodes.csv")


    var contentNum = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    numNodes.foreach { node =>
      contentNum = contentNum + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.get.mkStringP.replace("NaN", "0.0")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentNum, outputFolder, "numNodes.csv")


    var contentBool = "node_id,node_name,alias,domain_id,node_iri,ds_id,ds_name,node_attributes\n"

    boolNodes.foreach { node =>
      contentBool = contentBool + Seq(node.id, node.name, node.alias, node.domainID.getOrElse("NO_DOMAIN_ERROR"), node.iri, node.dsId, node.dsName, "\"" + node.profile.get.mkStringP.replace("NaN", "0.0")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentBool, outputFolder, "boolNodes.csv")

  }

  def writeNodeFilesPost(entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStrPost], outputFolder: String): Unit = {

    var contentEntity = "node_id,node_name,alias,node_iri,ds_id,ds_name,node_attributes\n"
    entityNodes.foreach { node =>

      contentEntity = contentEntity + Seq(node.id, node.name, node.alias, node.iri, node.dsId, node.dsName, "\"" +
        Seq.fill(profileColumns.length) {
          1
        }.mkString(",")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,node_iri,ds_id,ds_name,node_attributes\n"

    strAttNodes.foreach { node =>
      contentAttr = contentAttr + Seq(node.id, node.name,  node.iri, node.dsId, node.dsName, "\"" + node.profile).mkString(",") + "\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "strNodes.csv")
  }



  def writeNodeFilesPost(entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStrPost], numNodes:Seq[NodeNumPost], outputFolder: String): Unit = {

    var contentEntity = "node_id,node_name,alias,node_iri,ds_id,ds_name,node_attributes\n"
    entityNodes.foreach { node =>

      contentEntity = contentEntity + Seq(node.id, node.name, node.alias, node.iri, node.dsId, node.dsName, "\"" +
        Seq.fill(profileColumns.length) {
          1
        }.mkString(",")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,node_iri,ds_id,ds_name,node_attributes\n"

    strAttNodes.foreach { node =>
      contentAttr = contentAttr + Seq(node.id, node.name, node.iri, node.dsId, node.dsName, "\"" + node.profile).mkString(",") + "\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "strNodes.csv")

    var contentNum = "node_id,node_name,node_iri,ds_id,ds_name,node_attributes\n"

    numNodes.foreach { node =>
      contentNum = contentNum + Seq(node.id, node.name, node.iri, node.dsId, node.dsName, "\"" + node.profile).mkString(",") + "\"" + "\n"
    }
    writeFile(contentNum, outputFolder, "numNodes.csv")
  }


  def writeNodeFilesPost(entityNodes: Seq[NodeClass], strAttNodes: Seq[NodeStrPost], numNodes: Seq[NodeNumPost],boolNodes: Seq[NodeBoolPost], outputFolder: String): Unit = {

    var contentEntity = "node_id,node_name,alias,node_iri,ds_id,ds_name,node_attributes\n"
    entityNodes.foreach { node =>

      contentEntity = contentEntity + Seq(node.id, node.name, node.alias, node.iri, node.dsId, node.dsName, "\"" +
        Seq.fill(profileColumns.length) {
          1
        }.mkString(",")).mkString(",") + "\"" + "\n"
    }
    writeFile(contentEntity, outputFolder, "entityNodes.csv")

    var contentAttr = "node_id,node_name,node_iri,ds_id,ds_name,node_attributes\n"

    strAttNodes.foreach { node =>
      contentAttr = contentAttr + Seq(node.id, node.name, node.iri, node.dsId, node.dsName, "\"" + node.profile).mkString(",") + "\"" + "\n"
    }
    writeFile(contentAttr, outputFolder, "strNodes.csv")

    var contentNum = "node_id,node_name,node_iri,ds_id,ds_name,node_attributes\n"

    numNodes.foreach { node =>
      contentNum = contentNum + Seq(node.id, node.name, node.iri, node.dsId, node.dsName, "\"" + node.profile).mkString(",") + "\"" + "\n"
    }
    writeFile(contentNum, outputFolder, "numNodes.csv")

    var contentBool = "node_id,node_name,node_iri,ds_id,ds_name,node_attributes\n"

    boolNodes.foreach { node =>
      contentBool = contentBool + Seq(node.id, node.name, node.iri, node.dsId, node.dsName, "\"" + node.profile).mkString(",") + "\"" + "\n"
    }
    writeFile(contentBool, outputFolder, "boolNodes.csv")
  }

  def writeLinksFile(links: Seq[Link], outputFolder: String, fileName: String): Unit = {

    var linkContent = "src_id,dst_id,link_type,str_type,link_weight\n"
    links.foreach(l => linkContent = linkContent + Seq(l.src_id, l.target_id, l.linkType, l.typeStr, l.linkWeight).mkString(",") + "\n")
    writeFile(linkContent, outputFolder, fileName)

  }

  def writeInverseLinksFile(links: Seq[Link], outputFolder: String, fileName: String): Unit = {

    var linkContent = "src_id,dst_id,link_type,str_type,link_weight\n"
    links.foreach(l => linkContent = linkContent + Seq(l.target_id, l.src_id, l.linkType, l.typeStr, l.linkWeight).mkString(",") + "\n")
    writeFile(linkContent, outputFolder, fileName)

  }

}




case class IdAndFile(id: Int, file: String)

case class NodeClass(id: Int, iri: String, name: String, alias: String, dsId: Int, dsName: String)

case class NodeStrV2(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: String, var alias: Option[String], var domainID: Option[Int] = None, var domainURI: Option[String] = None)
case class NodeNumV2(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: String, var alias: Option[String], var domainID: Option[Int] = None, var domainURI: Option[String] = None)


case class NodeStr(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: Option[ProfilesRow], var alias: Option[String], var domainID: Option[Int] = None, var domainURI: Option[String] = None)
case class NodeNum(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: Option[ProfilesNumRow], var alias: Option[String], var domainID: Option[Int] = None, var domainURI: Option[String] = None)
case class NodeBool(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: Option[ProfilesBoolRow], var alias: Option[String], var domainID: Option[Int] = None, var domainURI: Option[String] = None)

case class NodeStrPost(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: String,  var domainID: Int)
case class NodeNumPost(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: String,  var domainID: Int)
case class NodeBoolPost(id: Int, iri: String, name: String, dsId: Int, dsName: String, var profile: String,  var domainID: Int)


case class NodeClass_old(id: Int, iri: String, name: String, alias: String)

case class NodeStr_old(id: Int, iri: String, name: String, var profile: Option[ProfilesRow], var alias: Option[String], var domainID: Option[Int] = None, var domainURI: Option[String] = None)

case class Link(src_id: Int, target_id: Int, linkType: String, typeStr: String, linkWeight: String = "1")

case class ResultRow(id: Int, filename: String, repository: String)

case class ProfilesRow(cardinality: String, uniqueness: String, entropy: String, incompleteness: String,
                       avg_length_string: String, longest_string: String, shortest_String: String,
                       avg_words_per_value: String, cardinality_words_per_col: String, max_words_per_string: String, min_words_per_string: String, sd_words_per_string: String,
                       very_short_pct: String, short_pct: String, medium_pct: String, long_pct: String, very_long_pct: String, alphabetic_pct: String,
                       alphanumeric_pct: String, numeric_pct: String, /* datetime_pct: String, */nonAlphanumeric_pct: String,
                       sd_frequency: String, min_frequency: String, max_frequency: String, constancy: String, avg_frequency: String,
                       octile_1: String, octile_2: String, octile_3: String, octile_4: String, octile_5: String, octile_6: String, octile_7: String,
                       max_pct_frequency: String, min_pct_frequency: String, sd_pct_frequency: String
                      ) {
  def mkStringP: String = List(
    cardinality, uniqueness, entropy, incompleteness,
    avg_length_string, longest_string, shortest_String,
    avg_words_per_value, cardinality_words_per_col, max_words_per_string, min_words_per_string, sd_words_per_string,
    very_short_pct, short_pct, medium_pct, long_pct, very_long_pct, alphabetic_pct,
    alphanumeric_pct, numeric_pct, /*datetime_pct,*/ nonAlphanumeric_pct,
    sd_frequency, min_frequency, max_frequency, constancy, avg_frequency,
    octile_1, octile_2, octile_3, octile_4, octile_5, octile_6, octile_7,
    max_pct_frequency, min_pct_frequency, sd_pct_frequency
  ).mkString(",")
}

case class ProfilesNumRow(
                           NumCardinality: String, NumUniqueness: String,
                           NumEntropy: String, NumIncompleteness: String,
                           AvgVal: String, MaxValue: String,
                           MinValue: String, NumNegatives: String,
                           NumPositives:String, Range:String,
                           SDValue: String, Variance:String,
                           Benford1:String, Benford2: String,
                           Benford3: String, Benford4: String,
                           Benford5: String, Benford6: String,
                           Benford7: String, Benford8: String,
                           Benford9: String, CoefficientVariation: String,
                           Kurtosis: String, MAD: String,
                           MedAD: String, Skewness: String,
                           Quartile1: String, Quartile2: String, Quartile3: String, Quartile4: String, IQR: String

                      ) {
  def mkStringP: String = List(
    NumCardinality, NumUniqueness, NumEntropy, NumIncompleteness,
    AvgVal, MaxValue, MinValue, NumNegatives, NumPositives,
    Range, SDValue, Variance, Benford1, Benford2, Benford3,
    Benford4, Benford5, Benford6, Benford7, Benford8, Benford9,
    CoefficientVariation, Kurtosis, MAD, MedAD, Skewness,
    Quartile1, Quartile2, Quartile3, Quartile4, IQR: String
  ).mkString(",")
}


case class ProfilesBoolRow(
                            containsTrue: String, ContainsFalse: String,
                         ) {
  def mkStringP: String = List(
    containsTrue, ContainsFalse
  ).mkString(",")
}

