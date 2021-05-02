package par2dataframes

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}

/*
  Aggregations & Grouping are Wide Transformation
  - Narrow Transformation: one input partition => one output partition
  - (Shuffle) Wide Transformation:
    - one/more input partitions => one/more output partitions
    - big performance impact
 */
object Topic_4_Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting, all the values except null
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  // or simply moviesDF.select(count("Major_Genre"))
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all the rows, include null
  moviesDF.select(count("*"))

  // counting distinct
  moviesDF.select(countDistinct("Major_Genre"))

  // approximate count, will not scan all the data set
  // useful in very big data set
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  moviesDF.select(min("IMDB_Rating"))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum("US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science, like: Mean, Standard Deviation and Variance
  moviesDF.select(
    avg("Rotten_Tomatoes_Rating"),
    mean("Rotten_Tomatoes_Rating"), // mean is the same as avg
    stddev("Rotten_Tomatoes_Rating") // stddev: how close or far the different values are to the mean value
  )

  // Grouping

  // select count(*) from moviesDF group by Major_Genre
  val countByGenreDSet: RelationalGroupedDataset = moviesDF.groupBy("Major_Genre") // includes null
  val countByGenreDF = countByGenreDSet.count()
  //  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  // 1. Sum up ALL the profits of ALL the movies in the DF
  moviesDF
    //    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .selectExpr("US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross")
    .select(sum("Total_Gross"))

  // 2. Count how many distinct directors we have
  moviesDF.select(countDistinct("Director"))

  // 3. Show the mean and standard deviation of US gross revenue for the movies
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  // 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
  // use column object to get descending order
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    ).orderBy(col("Avg_Rating").desc_nulls_last)
}
