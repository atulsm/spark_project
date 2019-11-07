name := "SparkTwoExperiments"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "2.1.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion %"provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming-flume_2.10" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.10" % sparkVersion %"provided",
  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion %"provided"
  
)