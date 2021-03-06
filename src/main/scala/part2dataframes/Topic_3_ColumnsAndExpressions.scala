package part2dataframes

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Column, SparkSession}

object Topic_3_ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //  carsDF.show()

  // Columns
  val firstColumn: Column = carsDF.col("Name")

  // selecting - new Data Frame (projecting: project carsDF to carNameDF)
  val carNameDF = carsDF.select(firstColumn)
  //  carNameDF.show()

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simpleExpression: Column = carsDF.col("Weight_in_lbs")
  val weightInKgExpression: Column = simpleExpression / 2.2
  val weightInKgExpressionV2: Column = expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    weightInKgExpressionV2
  )
  //  carsWithWeightDF.show()

  // selectExpr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )
  //  carsWithSelectExprWeightDF.show()

  // DF processing
  // add new column -> new DataFrame
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names, backticks to wrap words as a column name
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove columns
  carsWithColumnRenamed.drop("Cylinders", "Displacement")
  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings, single quote to wrap the value
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // union = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  //  allCountriesDF.show()

  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  //  moviesDF.show()

  // 1. Read the movies DF and select 2 columns of your choice
  val moviesTitleReleaseDF = moviesDF.select("Title", "Release_Date")
  val moviesTitleReleaseDF_2 = moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )
  val moviesTitleReleaseDF_3 = moviesDF.selectExpr("Title", "Release_Date")

  // 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
  // here cannot mix "" with (col("..") + col("..")..) together
  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross")
  )

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross"
  )

  val moviesProfitDF3 = moviesDF
    .select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross") + col("Worldwide_Gross"))

  // 3. Select all COMEDY movies with IMDB rating above 6
  val comediesDF = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val comediesDF2 = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comediesDF3 = moviesDF
    .select("Title", "IMDB_Rating")
    // single quote for String values
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

//  comediesDF3.show()
}
