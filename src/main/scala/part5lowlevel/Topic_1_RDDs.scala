package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import scala.io.Source

class Topic_1_RDDs extends App {
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

}
