

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class CiscoCustomer(id:Int,name:String,city:String)

object DFExp1 extends App {
   val sparkConf = new SparkConf().setAppName("Log Query").setMaster("local[2]")
   val sc = new SparkContext(sparkConf)
   val logRDD = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\customerData.txt") 
   val sqlContext1 = new SQLContext(sc)
   import sqlContext1.implicits._
   
   val custDF= logRDD.map(line => line.split(",")).map( row => new CiscoCustomer(row(0).toInt,row(1),row(2))).toDF()
  
   custDF.registerTempTable("CiscoCustomers")
   sqlContext1.sql("select name,city from CiscoCustomers where city='BLR'").show
   Thread.sleep(222222)
}