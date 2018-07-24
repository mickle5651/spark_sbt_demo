name := "spark_sbt_demo"

version := "0.1"

organization := "com.hisign.spark"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.10" % "1.6.0" % "compile",
	"org.apache.spark" % "spark-sql_2.10" % "1.6.0" % "compile",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "compile",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.0" % "compile",
	"org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0" % "compile"
)