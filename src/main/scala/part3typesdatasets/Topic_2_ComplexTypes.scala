package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Topic_2_ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  // set time parse to LEGACY, Spark 3.0's new Date Parser cannot parse '7-Aug-98' correctly
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  // to_date: convert StringType Column to DateType Column
  val moviesWithReleaseDates = moviesDF.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")
  )

  // new column
  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    // datediff, date_add, date_sub
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)

  // date in different format will be parsed to null
  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull) //.show()

  /**
   * Exercise
   * 1. How do we deal with multiple date formats?
   * 2. Read the stocks DF and parse the dates
   */

  // 1 - How do we deal with multiple date formats? - parse the DF multiple times, then union the small DFs

  // 2. Read the stocks DF and parse the dates
  val stockDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")
  stockDF.withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  // Structures (like Tuple) - {3000000, 3000000}

  // 1 - with col operators
  val movieProfitsDF = moviesDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
  )
  movieProfitsDF.select(
    col("Title"),
    // get field back from Structure
    col("Profit").getField("US_Gross").as("US_Profit")
  )

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays - [The, Land, Girls], not Structure
  val splitter = " |," // space or comma
  val moviesWithWords = moviesDF.select(
    col("Title"),
    split(col("Title"), splitter).as("Title_Words")
  )
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // Array item
    size(col("Title_Words")), // Array size
    array_contains(col("Title_Words"), "Love") // Array search - boolean
  ).show()
}
