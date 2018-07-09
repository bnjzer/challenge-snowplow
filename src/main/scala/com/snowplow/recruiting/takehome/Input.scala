package com.snowplow.recruiting.takehome

// Spark
import org.apache.spark.sql.{DataFrame, SparkSession}

// Util to help deal with problems when reading input
import scala.util.{Failure, Success, Try}

object Input {

  /** Reads the CSV file and creates the associated [[DataFrame]]. Checks if the required columns are all present.
    *
    * @param sparkSession [[SparkSession]] managing the [[DataFrame]] associated to the CSV file
    * @param path Path of the CSV file on disk
    * @param requiredColumns Columns that are required for further processing on the [[DataFrame]]
    * @return [[Success]] if the [[DataFrame]] was successfully created from the CSV file
    *        and contains all the required columns, [[Failure]] with the exception otherwise
    */
  def readCsvFile(sparkSession: SparkSession, path: String, requiredColumns: Seq[String]): Try[DataFrame] =
    Try {
      sparkSession.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    } match {
      case Success(houses) if requiredColumns.forall(houses.columns.contains) => Success(houses)
      case Failure(ex) => Failure(ex)
      case _ => Failure(new IllegalArgumentException(s"At least one of the required column is missing (${requiredColumns.mkString(", ")})"))
    }

}
