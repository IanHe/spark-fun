package par2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Topic_2_DataSources extends App {
  // create spark session
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Seq(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  //  val carsDF = spark.read.json("src/main/resources/data/cars.json")
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    // failFast: will throw Exception and crash eagerly once encounter malformed record
    // dropMalformed, permissive (default)
    .option("mode", "failFast")
    // can load path from S3 bucket
    .option("path", "src/main/resources/data/cars.json")
    .load()
}
