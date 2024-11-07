package test_prepro.pipeline_new

import java.io.IOException
import java.nio.file.{DirectoryStream, Files, Paths, SimpleFileVisitor}
import scala.jdk.CollectionConverters._

object DeleteGroundFolderByName {

  def main(args: Array[String]): Unit = {

    val basePath = "/Users/javierflores/Koofr/PhD/code/result/dataTest_v2"
    val directoryPath = s"${basePath}/ground_prueba_tmp"
    val searchString = "issued-building-permits_"

    // Get all directories from the specified path
    val dirStream: DirectoryStream[java.nio.file.Path] = Files.newDirectoryStream(Paths.get(directoryPath))
    val allFolders = dirStream.iterator().asScala.toList
    dirStream.close()

    // Filter out directories containing the searchString and delete them
    allFolders
      .filter(folder => folder.getFileName.toString.contains(searchString) && Files.isDirectory(folder))
      .foreach(folder => {
        println(s"Deleting folder: $folder")
        Files.walkFileTree(folder, new SimpleFileVisitor[java.nio.file.Path] {
          override def visitFile(file: java.nio.file.Path, attrs: java.nio.file.attribute.BasicFileAttributes): java.nio.file.FileVisitResult = {
            Files.delete(file)
            java.nio.file.FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: java.nio.file.Path, exc: IOException): java.nio.file.FileVisitResult = {
            Files.delete(dir)
            java.nio.file.FileVisitResult.CONTINUE
          }
        })
      })


  }

}
