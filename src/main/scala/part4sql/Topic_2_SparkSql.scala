package part4sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Topic_2_SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practices")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    // for Spark 2.4 users to allow overwrite tables: (Note: Spark 3 has to set 'path' option when save the table)
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val cardDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  cardDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  cardDF.createOrReplaceTempView("cars") // create an alias "cars" and refer it as a Table
  val americanCarsDF = spark.sql(
    """
      | select Name, Origin from cars where Origin = 'USA'
      |""".stripMargin)
  //  americanCarsDF.show()

  /*
    spark will create a folder spark-warehouse by default in the file tree
      -  you can config the directory: "spark.sql.warehouse.dir" in the spark session config
   */
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String): DataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: Seq[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    if(shouldWriteToWarehouse){
      // will save table in the specified data-warehouse folder
      tableDF.write
        /*
          For Spark 3, to overwrite existing table
          - set the path to the table's location (don't set to the DB's location)
         */
        .mode(SaveMode.Overwrite)
        .option("path", s"src/main/resources/warehouse/rtjvm.db/$tableName")
        .saveAsTable(tableName)
    }
  }

//  transferTables(Seq(
//    "employees",
//    "departments",
//    "titles",
//    "dept_emp",
//    "salaries",
//    "dept_manager"
//  ), true)

  // read DF from loaded Spark tables
//  val employeeDF2 = spark.read.table("employees")

  /**
   * Exercises
   *
   * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
   * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
   * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
   * 4. Show the name of the best-paying department for employees hired in between those dates.
   */

  // 1. Read the movies DF and store it as a Spark table in the rtjvm database.
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .option("path", "src/main/resources/warehouse/rtjvm.db/movies")
//    .saveAsTable("movies")

  // 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
  // require to loadTable and create view for employees
  transferTables(Seq("employees"))
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin
  )//.show()

  // 3. Show the average salaries for the employees hired in between those dates, grouped by department.
  // require to load tables and create views
  transferTables(Seq("employees", "dept_emp", "salaries"))
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin
  )//.show()

  // 4. Show the name of the best-paying department for employees hired in between those dates.
  // require to load tables and create views
  transferTables(Seq("employees", "dept_emp", "salaries", "departments"))
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = de.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin).show()
}
