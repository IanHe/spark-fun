package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Topic_1_CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))
  //  moviesDF.select(lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama" // equalTo, === is the same
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  // multiple ways of filtering: where(), filter()
  moviesDF.select("Title", "Major_Genre", "IMDB_Rating").where(goodRatingFilter)

  // select filtering as a boolean column (display: true of false)
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  // Numbers
  // math operators works on numeric columns, will put null on other column types
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
  //  moviesAvgRatingsDF.show()

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))) //.show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen") or col("Name").contains("vw"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract") // will drop the column in the result, which is quite common for regex column
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /**
   * Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *   - contains
   *   - regexes
   */
  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames.mkString("|").toLowerCase // volskwagen|mercedes-benz|ford
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")

  // version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.foldRight(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter)
}
