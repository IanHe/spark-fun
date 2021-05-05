package part3typesdatasets

import org.apache.spark.sql.SparkSession

/**
 * Datasets:
 * Typed DataFrames: distributed collections of jvm objects
 */
object Topic_4_Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  // set time parse to LEGACY, Spark 3.0's new Date Parser cannot parse '7-Aug-98' correctly
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val numbersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
}
