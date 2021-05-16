package part7bigdata

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object Topic_1_TaxiApplication extends App {
  val spark = SparkSession.builder()
    .appName("TaxiApplication")
    .config("spark.master", "local")
    .config("spark.sql.session.timeZone", "America/New_York") // the taxi data is based on New York
    .getOrCreate()


  // read parquet
  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  //  taxiDF.printSchema()
  //  taxiDF.show()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  //  taxiZonesDF.printSchema()

  /**
   * Questions:
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   * 2. What are the peak hours for taxi?
   * 3. How are the trips distributed by length? Why are people taking the cab?
   * 4. What are the peak hours for long/short trips?
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   * 6. How are people paying for the ride, on long/short trips?
   * 7. How is the payment type evolving with time?
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   */

  // 1. Which zones have the most pickups/dropoffs overall?
  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone") // LocationID is the same of PULocationID
    .orderBy(col("totalTrips").desc_nulls_last)
  //  pickupsByTaxiZoneDF.show()

  // 1b - group by borough
  val pickupsByBoroughDF = pickupsByTaxiZoneDF.groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  //  pickupsByBoroughDF.show()

  // 2. What are the peak hours for taxi?
  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  //      pickupsByHourDF.show()

  // 3. How are the trips distributed by length? Why are people taking the cab?
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )
  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()
  //  tripsByLengthDF.show()

  // 4. What are the peak hours for long/short trips?
  val pickupsByHourByLengthDF = tripsWithLengthDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  //  pickupsByHourByLengthDF.show(48)

  // 5. What are the top 3 pickup/dropoff zones for long/short trips?
  def pickupDropoffPopularity(predicate: Column) = tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)

  //  pickupDropoffPopularity(col("isLong")).show()
  //  pickupDropoffPopularity(not(col("isLong"))).show()

  // 6. How are people paying for the ride, on long/short trips?
  /*
    RatecodeID:
    - 1 = credit card
    - 2 = cash
    - 3 = no charge
    - 4 = dispute
    - 5 = unknown
    - 6 = voided
    - 99 = ???
   */
  val ratecodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  //  ratecodeDistributionDF.show()

  // 7. How is the payment type evolving with time?
  val ratecodeEvolution = taxiDF
    // to_date exclude the time
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))
  //  ratecodeEvolution.show()

  // 8. Can we explore a ride-sharing opportunity by grouping close short trips?
  /*
    - suppose a five minutes timeframe for people grouping
   */
  val passengerCountDF = taxiDF.where(col("passenger_count") < 3).select(count("*"))
  passengerCountDF.show()

  val groupAttemptsDF = taxiDF
    .select(
      // the unix timestamp in seconds divide by 5 minutes
      round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
      col("PULocationID"),
      col("total_amount")
    )
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(
      count("*").as("total_trips"),
      sum(col("total_amount")).as("total_amount")
    )
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
  //  groupAttemptsDF.show()

  /*
    A simple model for estimating potential economic impact over the dataset
    assume:
    - 5% of taxi trips detected to be groupable at any time
    - 30% of people actually accept to be grouped
    - $5 discount if you take a grouped ride
    - $2 extra to take an individual ride (privacy/time)
    - if two rides grouped, reducing cost by 60% of one average ride
   */
  import spark.implicits._
  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  // taxiDF.select(avg(col("total_amount"))).as[Double] is a Dataset[Double]
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0) // import spark.implicits._

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))
//  groupingEstimateEconomicImpactDF.show(100)
  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  totalProfitDF.show()
}
