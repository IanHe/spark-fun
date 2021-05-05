name := "spark-essentials-fun"

version := "0.1"

scalaVersion := "2.12.13"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  library.sparkCore,
  library.sparkSql,
  library.scalaLogging,
  library.postgresql
)

parallelExecution := false

lazy val library = new {
  object Version {
    val spark = "3.1.1"
    val scalaLogging = "3.9.3"
    val postgresql = "42.2.20"
  }

  val sparkCore = "org.apache.spark" %% "spark-core" % Version.spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  val postgresql = "org.postgresql" % "postgresql" % Version.postgresql
}

