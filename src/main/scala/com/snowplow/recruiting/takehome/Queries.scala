package com.snowplow.recruiting.takehome

// Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Queries {

  implicit class RichDataFrame(df: DataFrame) {

    /** Computes the neighborhood distribution.
      *
      * @param colNeighborhood Name of the column containing the neighborhood in the input [[DataFrame]]
      * @param colCount Name of the column containing the count for each neighborhood in the output [[DataFrame]]
      * @return [[DataFrame]] with 2 columns: neighborhood, number of houses
      */
    def neighborhoodDist(colNeighborhood: String, colCount: String): DataFrame =
      df.groupBy(colNeighborhood)
        .count()
        .orderBy(desc(colCount))

    /** Computes the average sale price per neighborhood.
      *
      * @param colNeighborhood Name of the column containing the neighborhood in the input [[DataFrame]]
      * @param colSalePrice Name of the column containing the sale price in the input [[DataFrame]]
      * @param colAvgSalePrice Name of the column containing the average sale price per neighborhood in the output [[DataFrame]]
      * @param priceDecimals Number of decimals for the prices in the output [[DataFrame]]
      * @return [[DataFrame]] with 2 columns: neighborhood, average price
      */
    def avgSalePricePerNeighborhood(
        colNeighborhood: String,
        colSalePrice: String,
        colAvgSalePrice: String,
        priceDecimals: Int): DataFrame =

      df.groupBy(colNeighborhood)
        .agg(format_number(avg(colSalePrice), priceDecimals).alias(colAvgSalePrice))
        .orderBy(desc(colAvgSalePrice))

    /** Selects a list of columns for the least and most expensive houses.
      *
      * @param colSalePrice Name of the column containing the sale price in the input [[DataFrame]]
      * @param nbMostExpensiveHouses How many most expensive houses in the output [[DataFrame]]
      * @param nbLeastExpensiveHouses How many least expensive houses in the output [[DataFrame]]
      * @param colsToKeep Names of the columns we want to keep for the least and most expensive houses
      *                   in the output [[DataFrame]]
      * @return First [[DataFrame]] with the most expensive houses, second with the least expensive
      */
    def mostLeastExpensiveHouses(
        colSalePrice: String,
        nbMostExpensiveHouses: Int,
        nbLeastExpensiveHouses: Int,
        colsToKeep: Seq[String]): (DataFrame, DataFrame) = {

      val houses = df.select(colsToKeep.head, colsToKeep.tail: _*)
      houses.persist(StorageLevel.MEMORY_ONLY) // use for both least and most expensive

      val mostExpensive =
        houses.sort(desc(colSalePrice))
          .limit(nbMostExpensiveHouses)

      val leastExpensive =
        houses.sort(asc(colSalePrice))
          .limit(nbLeastExpensiveHouses)

      (mostExpensive, leastExpensive)
    }

    /** Computes the percentage of houses with a big garage among the houses with the best overall condition.
      *
      * @param colConditionRating Name of the column containing the rating of the overall condition in the input [[DataFrame]]
      * @param colGarageSize Name of the column containing the garage size in the input [[DataFrame]]
      * @param thresholdGoodCondition From which threshold we consider that a house has a good overall condition
      * @param thresholdBigGarage Minimum size of the garage (in square feet) to be considered big
      * @param percentageDecimals Number of decimals for the percentage in the output [[DataFrame]]
      * @param colGoodPercentageBigGarage Name if the column containing the percentage in the output [[DataFrame]]
      * @return [[DataFrame]] with 1 column and 1 value: percentage of houses with a big garage
      *         among the houses with the best overall condition
      */
    def goodPercentageBigGarage(
        colConditionRating: String,
        colGarageSize: String,
        thresholdGoodCondition: Int,
        thresholdBigGarage: Int,
        percentageDecimals: Int,
        colGoodPercentageBigGarage: String): DataFrame =

      df.filter(col(colConditionRating) > thresholdGoodCondition)
        .select(format_number(count(when(col(colGarageSize) > thresholdBigGarage, true)) / count("*"), percentageDecimals)
        .alias(colGoodPercentageBigGarage))

    /** Imputes the missing frontages.
      * The strategy is to first compute a [[DataFrame]] with the average ratio between the size of a lot and its frontage
      * for each combination of lot configuration and lot shape.
      * We can then merge this [[DataFrame]] with the one containing the missing frontages and use the computed ratios
      * to impute them.
      * Note: if a lot with a missing frontage has a combination of shape and configuration which doesn't exist in the
      * existing frontages, it will not be imputed. A fallback could be to use the average ratio computed for each
      * lot configuration for instance, but using only one parameter would be even less accurate.
      *
      * @param colFrontage Name of the column containing the linear feet of street connected to property
      *                    in the input [[DataFrame]]
      * @param colConfig Name of the column containing the configuration of the lot in the input [[DataFrame]]
      * @param colArea Name of the column containing the size of the lot in square feet in the input [[DataFrame]]
      * @param colShape Name of the column containing the shape of the lot in the input [[DataFrame]]
      * @param emptyFrontage String that identifies a missing frontage in the input [[DataFrame]]
      * @return [[DataFrame]] containing ONLY the lots for which the frontage has been imputed
      */
    def imputeFrontage(
        colFrontage: String,
        colConfig: String,
        colArea: String,
        colShape: String,
        emptyFrontage: String): DataFrame = {

      val colAvgRatio = "AvgRatio"

      val avgRatios = df.filter(not(col(colFrontage) === emptyFrontage))
        .groupBy(colConfig, colShape)
        .agg(avg(col(colArea) / col(colFrontage)).alias(colAvgRatio))

      df.filter(col(colFrontage) === emptyFrontage)
        .join(avgRatios, df(colConfig) === avgRatios(colConfig) && df(colShape) === avgRatios(colShape), "inner")
        .withColumn(colFrontage, col(colArea) / col(colAvgRatio))
        .drop(colAvgRatio)
    }

    /** Computes the average sale price per square feet, per neighborhood and per year.
      *
      * @param colNeighborhood Name of the column containing the neighborhood in the input [[DataFrame]]
      * @param colSalePrice Name of the column containing the sale price in the input [[DataFrame]]
      * @param colArea Name of the column containing the size of the lot in square feet in the input [[DataFrame]]
      * @param priceDecimals Number of decimals for the prices in the output [[DataFrame]]
      * @param colYearSold Name of the column containing the year the house was sold in the input [[DataFrame]]
      * @param colAvgPricePerSquareFeet Name of the column containing the average price per square feet
      *                                 in the output [[DataFrame]]
      * @return [[DataFrame]] with three columns: neighborhood, year, average sale price per square feet
      *         for this year and this neighborhood
      */
    def salePriceEvolution(
        colNeighborhood: String,
        colSalePrice: String,
        colArea: String,
        priceDecimals: Int,
        colYearSold: String,
        colAvgPricePerSquareFeet: String): DataFrame = {

      val colPricePerSquareFeet = "PricePerSquareFeet"

      df.withColumn(colPricePerSquareFeet, col(colSalePrice) / col(colArea))
        .groupBy(colNeighborhood, colYearSold)
        .agg(avg(colPricePerSquareFeet).alias(colAvgPricePerSquareFeet))
        .withColumn(colAvgPricePerSquareFeet, format_number(col(colAvgPricePerSquareFeet), priceDecimals))
        .orderBy(col(colNeighborhood), desc(colYearSold))
    }

    /** Computes how many houses there are in each neighborhood according to a list of basic criteria :
      * family size, number of cars and budget. We could then recommend the neighborhood where there is the most
      * houses respecting the criteria.
      * This is very naive because each family would have different additional criteria to take into account.
      *
      * @param familySize Number of persons in the family
      * @param nbCars Number of cars. It's assumed that they all have to fit in the garage
      * @param budget Available budget to buy
      * @param budgetMarginPercent Until how many percent more of the budget we propose a house
      * @param colBedroom Name of the column containing the number of bedrooms
      *                   (excluding in basement) in the input [[DataFrame]]
      * @param colGarageCars Name of the column containing the number of cars that fit in the garage
      *                      in the input [[DataFrame]]
      * @param colSalePrice Name of the column containing the sale price in the input [[DataFrame]]
      * @param colNeighborhood Name of the column containing the neighborhood in the input [[DataFrame]]
      * @param colCount Name of the column containing the count for each neighborhood in the output [[DataFrame]]
      * @return [[DataFrame]] with 2 columns: neighborhood, number of houses respecting the criteria
      */
    def neighborhoodRecommendation(
        familySize: Int,
        nbCars: Int,
        budget: Int,
        budgetMarginPercent: Int,
        colBedroom: String,
        colGarageCars:String,
        colSalePrice: String,
        colNeighborhood: String,
        colCount: String): DataFrame =

      df.filter(col(colBedroom) >= Math.max(1, familySize - 1)) // parents in the same room, someone alone one bedroom
        .filter(col(colGarageCars) >= nbCars)
        .filter(col(colSalePrice) <= (100 + budgetMarginPercent) * budget / 100)
        .neighborhoodDist(colNeighborhood, colCount)
        .orderBy(desc(colCount))
  }
}
