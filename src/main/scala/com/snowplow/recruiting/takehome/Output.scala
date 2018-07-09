package com.snowplow.recruiting.takehome

// Spark
import org.apache.spark.sql.DataFrame

// Logging
import org.slf4j.LoggerFactory

// Writing of the output file
import java.io.{File, PrintWriter}
import java.nio.file.Paths

object Output {

  private lazy val logger =  LoggerFactory.getLogger(getClass)

  /** Creates the CSV string associated to a [[DataFrame]] and writes it both to the logger and to a file
    * on the Spark master.
    *
    * @param df Output [[DataFrame]]
    * @param outputPath Path on disk to write the CSV file
    * @param filename Name of the CSV file written to disk
    * @param elementsSeparator Between each column of a row
    * @param linesSeparator Between each row
    */
  def processResult(
      df: DataFrame,
      outputPath: String,
      filename: String,
      elementsSeparator: String,
      linesSeparator: String): Unit = {

    val result = df.collect()

    if (result.length > 0) {
      val outputStr =
        df.columns.mkString(elementsSeparator) +
          linesSeparator +
          result.map(_.mkString(elementsSeparator)).mkString(linesSeparator)

      logger.info(outputStr)

      // This should be way cleaner in a production environment
      if (!new File(outputPath).exists()) new File(outputPath).mkdirs()
      val outputFile = Paths.get(outputPath, filename).toString
      new File(outputFile).delete()
      val fileWriter = new PrintWriter(outputFile)
      fileWriter.println(outputStr)
      fileWriter.close()
    }
  }

}
