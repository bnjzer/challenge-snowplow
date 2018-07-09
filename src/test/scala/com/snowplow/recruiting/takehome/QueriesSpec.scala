package com.snowplow.recruiting.takehome

// Spark testing
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class QueriesSpec extends UnitSpec with DataFrameSuiteBase {

  import sqlContext.implicits._ // to get toDF to create DataFrames

  import Queries._

  "Query for most and least expensive houses" should
      "return the correct number of correct houses with the expected columns" in {

    val colSalePrice = "salePrice"
    val nbMostExpensiveHouses = 3
    val nbLeastExpensiveHouses = 2
    val colA = "a"
    val colB = "b"
    val colC = "c"
    val inputCols = Seq(colSalePrice, colA, colB, colC)
    val colsToKeep = Seq(colSalePrice, colA, colC)

    val first = (1000, "abc", "def", 1)
    val second = (2000, "jkl", "mno", 2)
    val third = (3000, "stu", "vwx", 3)
    val fourth = (4000, "bcd", "efg", 4)
    val input = sc.parallelize(List(second, first, fourth, third))
      .toDF(inputCols: _*)

    val expectedMostExpensive = sc.parallelize(List(fourth, third, second))
      .toDF(inputCols: _*)
      .select(colsToKeep.head, colsToKeep.tail: _*)

    val expectedLeastExpensive = sc.parallelize(List(first, second))
      .toDF(inputCols: _*)
      .select(colsToKeep.head, colsToKeep.tail: _*)

    val (actualMostExpensive, actualLeastExpensive) = input.mostLeastExpensiveHouses(
      colSalePrice,
      nbMostExpensiveHouses,
      nbLeastExpensiveHouses,
      colsToKeep
    )

    assertDataFrameEquals(expectedMostExpensive, actualMostExpensive)
    assertDataFrameEquals(expectedLeastExpensive, actualLeastExpensive)
  }
}
