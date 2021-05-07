package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.io.Source

object Topic_1_RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(fileName: String): Seq[StockValue] = Source.fromFile(fileName)
    .getLines()
    .drop(1) // drop the header row in csv
    .map(line => line.split(","))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
    .toSeq

  val stocksRDD: RDD[StockValue] = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2: RDD[StockValue] = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // filter out the header
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS: Dataset[StockValue] = stocksDF.as[StockValue]
  val stocksRDD3: RDD[StockValue] = stocksDS.rdd

  // RDD -> DF
  // specify the column name for multiple columns
  // because DataFrame is Dataset[Row], when transfer to DataFrame you loose the data type
  val numbersDF: DataFrame = numbersRDD.toDF("numbers")

  // RDD -> DS
  // when transfer to Dataset, the data type is kept
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD)

  // Transformation
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount: Long = msftRDD.count() // eager Action
  val companyNamesRDD: RDD[String] = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((a: StockValue, b: StockValue) => a.price < b.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping: very expensive which will involve operation: Shuffling
  val groupedStockRDD: RDD[(String, Iterable[StockValue])] = stocksRDD.groupBy(_.symbol)

  // Partitioning
  /*
    Repartition:
     - EXPENSIVE. Involves Shuffling. Best practice: partition EARLY, then process that.
       Size of a partition 10-100MB.
     - below will generate 30 partition files, when save the file you can see it
   */
  val repartitionedStocksRDD: RDD[StockValue] = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    // when use parquet() to save file, do not have to specify path option
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  /*
    coalesce:
    - does NOT involve shuffling, because data does not to be moved in between the entire cluster
      in below case, at least 15 partitions are going to stay in the same place,
      the other partitions are going to move data to them
    - will generate new RDD with 15 partition files
   */
  val coalescedRDD = repartitionedStocksRDD.coalesce(15)
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   *
   * 1. Read the movies.json as an RDD.
   * 2. Show the distinct genres as an RDD.
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   * 4. Show the average rating of movies by genre.
   */
  case class Movie(title: String, genre: String, rating: Double)

  //  1. Read the movies.json as an RDD.
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  val moviesRDD = moviesDF
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    )
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2. Show the distinct genres as an RDD.
  val genresRDD: RDD[String] = moviesRDD.map(_.genre).distinct()

  // 3. Select all the movies in the Drama genre with IMDB rating > 6.
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  // one way to show the RDd
  //  goodDramasRDD.toDF().show()

  // 4. Show the average rating of movies by genre.
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies: Iterable[Movie]) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }
  avgRatingByGenreRDD.toDF().show()
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show

  /*
  reference:
    +-------------------+------------------+
    |              genre|       avg(rating)|
    +-------------------+------------------+
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |        Documentary| 6.997297297297298|
    |       Black Comedy|6.8187500000000005|
    |  Thriller/Suspense| 6.360944206008582|
    |            Musical|             6.448|
    |    Romantic Comedy| 5.873076923076922|
    |Concert/Performance|             6.325|
    |             Horror|5.6760765550239185|
    |            Western| 6.842857142857142|
    |             Comedy| 5.853858267716529|
    |             Action| 6.114795918367349|
    +-------------------+------------------+

  RDD:
    +-------------------+------------------+
    |              genre|            rating|
    +-------------------+------------------+
    |Concert/Performance|             6.325|
    |            Western| 6.842857142857142|
    |            Musical|             6.448|
    |             Horror|5.6760765550239185|
    |    Romantic Comedy| 5.873076923076922|
    |             Comedy| 5.853858267716529|
    |       Black Comedy|6.8187500000000005|
    |        Documentary| 6.997297297297298|
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |  Thriller/Suspense| 6.360944206008582|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
 */
}
