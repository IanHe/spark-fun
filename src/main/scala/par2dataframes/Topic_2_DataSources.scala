package par2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
  val carsDF: DataFrame = spark.read
    .format("json")
    .schema(carsSchema)
    /*
      Option: mode
      -- failFast: will throw Exception and crash eagerly once encounter malformed record
      -- dropMalformed: will drop the malformed row
      -- permissive (default): will keep the row, but parse the malformed column as null
     */
    .option("mode", "failFast")
    // can load path from S3 bucket
    .option("path", "src/main/resources/data/cars.json")
    .load()

  //  carsDF.show() // the action will trigger the Data Frame to load

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
  Writing DFs
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  - path (different from read, this is the folder where the files will be saved)
  - zero or more options
 */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe")

  // JSON flags
  val carsDF_2 = spark.read
    .schema(carsSchema)
    /*
      option: dateFormat
      - Use small 'y' instead of the big 'Y' for the year, Spark 3.0 needs an additional configuration to parse 'Y' correctly
      - if migrating Spark 2 to Spark 3, use:
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") in the SparkSession
      - if not specify dateFormat, spark use default ISO dateFormat
      - couple with schema; if Spark fails parsing, it will put null
     */
    .option("dateFormat", "yyyy-MM-dd")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // uncompressed(default), bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  carsDF_2.show()
  carsDF_2.printSchema()

  // CSV flags
  val stockSchema = StructType(Seq(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stockDF = spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM d yyyy")
    /*
       CSV specific options
       - header: whether the header is in the csv file
       - sep: seperator used in the csv file, default is ","
       - nullValue: CSV does not has null, so the parse will parse nullValue to this option specified value
     */
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

//  stockDF.show()
//  stockDF.printSchema()

  // Parquet
  /*
    - an open source compressed binary data storage format, optimized for fast-reading of columns
    - it works very well with spark and it is the default storage format for Data Frames
    - advantages:
      - it is very predictable, it does not need options like in CSV
      - compressed binary data, storage size is very small, 1/6 - 1/10 of the original size
   */
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")
//    .save("src/main/resources/data/cars.parquet") // the default save is Parquet

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"
  val employeesTable = "public.employees"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", employeesTable)
    .load()
  employeesDF.show()

  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // 1 Write to tab-separated values file
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies-csv")

  // 2 Write to Parquet
  moviesDF.write.save("src/main/resources/data/movies-parquet")

  // 3 Write to remote DB
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
