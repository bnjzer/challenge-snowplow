package com.snowplow.recruiting.takehome

object Config {

  case class JobConfig(
    inputFile: String,
    outputPath: String,
    inputCols: InputColsConfig,
    queries: QueriesConfig,
    output: OutputConfig
  )

  case class InputColsConfig(
    neighborhood: String,
    salePrice: String,
    dwellingType: String,
    dwellingStyle: String,
    conditionRating: String,
    constructionDate: String,
    nbBedroom: String,
    garageSize: String,
    poolSize: String,
    frontage: String,
    config: String,
    area: String,
    shape: String,
    yearSold: String,
    garageCars: String
  )

  case class QueriesConfig(
    thresholdGoodCondition: Int,
    thresholdBigGarage: Int,
    nbMostExpensiveHouses: Int,
    nbLeastExpensiveHouses: Int,
    columnsMostLeastExpensive: Seq[String],
    emptyFrontage: String,
    familySize: Int,
    nbCars: Int,
    budget: Int,
    budgetMarginPercent: Int
  )

  case class OutputConfig(
    cols: OutputColsConfig,
    format: OutputFormatConfig,
    files: OutputFilesConfig
  )

  case class OutputColsConfig(
    avgSalePrice: String,
    count: String,
    goodPercentageBigGarage: String,
    avgPricePerSquareFeet: String
  )

  case class OutputFormatConfig(
    priceDecimals: Int,
    percentageDecimals: Int,
    elementsSeparator: String,
    linesSeparator: String
  )

  case class OutputFilesConfig(
    neighborhoodDistribution: String,
    neighborhoodAvgPrice: String,
    mostExpensiveHouses: String,
    leastExpensiveHouses: String,
    goodPercentageBigGarage: String,
    imputedFrontages: String,
    salePriceEvolution: String,
    neighborhoodRecommendation: String
  )

}