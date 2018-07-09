package com.snowplow.recruiting.takehome

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

// For input error handling
import scala.util.{Failure, Success}

// Pureconfig
import pureconfig.loadConfigOrThrow

// Take home
import Config._
import Input._
import Queries._
import Output._

object Main {

  def main(args: Array[String]): Unit = {

    lazy val logger = LoggerFactory.getLogger(getClass)

    // Load config
    val conf = loadConfigOrThrow[JobConfig]

    // Create spark session
    val sparkConf = new SparkConf()
      .setAppName("Take home Ben")
      .setIfMissing("spark.master", "local[*]")

    lazy val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    // Read input
    val requiredColumns = conf.inputCols.productIterator.toSeq.map(_.toString)
    readCsvFile(spark, conf.inputFile, requiredColumns) match {
      case Success(houses) =>
        houses.persist(StorageLevel.MEMORY_ONLY) // will be used as input by many computations

        // 1. What is the neighborhood distribution like?
        val neighborhoodDistribution = houses.neighborhoodDist(conf.inputCols.neighborhood, conf.output.cols.count)

        // 2. What is the average sale price per neighborhood?
        val neighborhoodAvgPrice = houses.avgSalePricePerNeighborhood(
          conf.inputCols.neighborhood,
          conf.inputCols.salePrice,
          conf.output.cols.avgSalePrice,
          conf.output.format.priceDecimals
        )

        // 3. What are the 10 most expensive houses like?
        // 4. What are the 10 least expensive houses like?
        val (mostExp, leastExp) = houses.mostLeastExpensiveHouses(
          conf.inputCols.salePrice,
          conf.queries.nbMostExpensiveHouses,
          conf.queries.nbLeastExpensiveHouses,
          conf.queries.columnsMostLeastExpensive
        )

        // 5. Among the houses with the best overall condition, what percentage have a garage area bigger than 1000 square feet?
        val goodPercentageBigGarage = houses.goodPercentageBigGarage(
          conf.inputCols.conditionRating,
          conf.inputCols.garageSize,
          conf.queries.thresholdGoodCondition,
          conf.queries.thresholdBigGarage,
          conf.output.format.percentageDecimals,
          conf.output.cols.goodPercentageBigGarage
        )

        // 6. How big is my frontage?
        val imputedFrontages = houses.imputeFrontage(
          conf.inputCols.frontage,
          conf.inputCols.config,
          conf.inputCols.area,
          conf.inputCols.shape,
          conf.queries.emptyFrontage
        )

        // 7. Can you share one insightful nugget of information from your data exploration?
        val salePriceEvolutionPerNeighborhood = houses.salePriceEvolution(
          conf.inputCols.neighborhood,
          conf.inputCols.salePrice,
          conf.inputCols.area,
          conf.output.format.priceDecimals,
          conf.inputCols.yearSold,
          conf.output.cols.avgPricePerSquareFeet
        )

        // 8. In which neighborhood would you advise a family of 5 with 2 cars with a budget of 170 000 $ to look for a house?
        val neighborhoodRecommendation = houses.neighborhoodRecommendation(
          conf.queries.familySize,
          conf.queries.nbCars,
          conf.queries.budget,
          conf.queries.budgetMarginPercent,
          conf.inputCols.nbBedroom,
          conf.inputCols.garageCars,
          conf.inputCols.salePrice,
          conf.inputCols.neighborhood,
          conf.output.cols.count
        )

        // Process the results
        Map(
          conf.output.files.neighborhoodDistribution -> neighborhoodDistribution,
          conf.output.files.neighborhoodAvgPrice -> neighborhoodAvgPrice,
          conf.output.files.mostExpensiveHouses -> mostExp,
          conf.output.files.leastExpensiveHouses -> leastExp,
          conf.output.files.goodPercentageBigGarage -> goodPercentageBigGarage,
          conf.output.files.imputedFrontages -> imputedFrontages,
          conf.output.files.salePriceEvolution -> salePriceEvolutionPerNeighborhood,
          conf.output.files.neighborhoodRecommendation -> neighborhoodRecommendation
        )
        .foreach(t2 =>
          processResult(
            t2._2,
            conf.outputPath,
            t2._1,
            conf.output.format.elementsSeparator,
            conf.output.format.linesSeparator
          )
        )
      case Failure(ex) =>
        logger.error("Problem while reading the input file", ex)
        System.exit(1)
    }
  }

}
