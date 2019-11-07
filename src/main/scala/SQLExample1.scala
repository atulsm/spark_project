import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel


case class customer(id:Int,name:String,city:String)

object SQLExample1 extends App {  
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  
   val logFile= "C:\\BigData\\Apache Spark\\Customers.csv"
    
    val conf = new SparkConf().setAppName("SQL Application").setMaster("local[2]") 
    val sc1 = new SparkContext(conf)
   // val logData = sc1.textFile(logFile)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc1)
    import sqlContext.implicits._
    
    val dfCustomersDF= sc1.textFile(logFile).map(_.split(",")).repartition(4)
			.map( myvalue => new customer(myvalue(0).trim.toInt,myvalue(1),myvalue(2))).toDF
			
			
			
			dfCustomersDF.createOrReplaceTempView("atos_clients")
			
			dfCustomersDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
			
			//sqlContext.cacheTable("atos_clients")
			
			//val myRdd= dfCustomersDF.rdd
			
		 
		  

		  
		  dfCustomersDF.filter(dfCustomersDF("city").equalTo("BLR")).show
		  
		 //dfCustomersDF.groupBy("city").count().show
		 
		  
			Thread.sleep(200000)
}