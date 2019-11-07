

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SalesGroupByCustID extends App {

  val rootLogger = Logger.getLogger("org")
  rootLogger.setLevel(Level.ERROR)

  val logFile = "C:/BigData/Apache Spark/build-example/sales.txt"

  val conf = new SparkConf().setAppName("Sales Application").setMaster("local[2]");
  val sc = new SparkContext(conf)
  val logData = sc.textFile(logFile, 3)
  logData.map(line => line.split(",")).map(row => (row(1).toInt, row(3).toLong)).reduceByKey(_ + _).saveAsTextFile("/spark_sales_out")

}