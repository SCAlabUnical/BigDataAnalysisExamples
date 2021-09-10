name := "ChurnPredictionSystemSpark"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-mllib" % "2.4.3",
  "org.apache.spark" %% "spark-hive" % "2.4.3" ,
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0",
  "com.typesafe" % "config" % "1.4.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0")


