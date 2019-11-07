

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SQLExample2 extends App {

  val logFile = "C:\\BigData\\Apache Spark\\Customers.csv"

  val conf = new SparkConf().setAppName("Simple Application")
    .setMaster("local[2]")
  val sc1 = new SparkContext(conf)
  //val logData = sc1.textFile(logFile)
  val customer_schema = "id name city"
  val schema = StructType(customer_schema.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

  val sqlContext = new org.apache.spark.sql.SQLContext(sc1)
  val rddCustomer = sc1.textFile(logFile)
  val rowRDD = rddCustomer.map(_.split(",")).map(p => Row(p(0), p(1), p(2)))

  var customer_DF = sqlContext.createDataFrame(rowRDD, schema)

  customer_DF.createOrReplaceTempView("subex_customers_new")
  customer_DF.select("name", "city").show

}