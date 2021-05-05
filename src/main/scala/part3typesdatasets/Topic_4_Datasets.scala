package part3typesdatasets

import org.apache.spark.sql.functions.{array_contains, avg, col}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import java.sql.Date

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
  //  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val numbersDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ < 100)

  // dataset of a complex type

  // 1 - define your case class
  case class Car(
    Name: String,
    Miles_per_Gallon: Option[Double],
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Option[Long],
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: Date,
    Origin: String
  )

  // 2 - read the DF from the file

  def readDF(filename: String): DataFrame = spark.read
    .option("inferSchema", "true") // the default "inferSchema" will read all number as Long
    .option("dateFormat", "yyyy-MM-dd")
    .json(s"src/main/resources/data/$filename")

  def readWithSchemaDF(filename: String, schema: StructType): DataFrame = spark.read
    .option("inferSchema", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(schema) // some schema has complex attribute type like Date, so specify the schema is quite important for parsing
    .json(s"src/main/resources/data/$filename")

  val carSchema = Encoders.product[Car].schema
  val carsDF = readWithSchemaDF("cars.json", carSchema)

  // 3 - define an encoder (importing the implicits)

  import spark.implicits._

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]
  carsDS.filter(_.Year.before(Date.valueOf("1970-01-02")))

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
   * Exercises
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

  // 1. Count how many cars we have
  val carsCount = carsDS.count
  println(carsCount)

  // 2. Count how many POWERFUL cars we have (HP > 140)
  carsDS.filter(_.Horsepower.exists(_ > 140)).count()
  //println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3. Average HP for the entire dataset
  // - attention: put 0L instead of 0
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)
  // also use the DF functions!
  carsDS.select(avg(col("Horsepower"))).show()

  // Joins
  // - attention, define number as Long here, because Scala by default parse number as Long using inferSchema
  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
    // join =>(will loose type) DataFrame, joinWith => DataSet, by default is inner join
    .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  //  guitarPlayerBandsDS.show()

  /**
   * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)
   */
  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // Grouping DS
  val carsGroupBy: KeyValueGroupedDataset[String, Car] = carsDS.groupByKey(_.Origin) // not groupBy
  carsGroupBy.count().show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations
}
