

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

object SqlEx2 {

  def main(args: Array[String]) {
    // val logFile = "file:////usr/hdp/2.4.0.0-169/spark/README.md" // Should be some file on your system set("spark.ui.port", "44040" ).

    val logFile = "/tmp/Customers.csv"

    val conf = new SparkConf().setAppName("SqlExample1").setMaster("local[2]")
    val sc1 = new SparkContext(conf)
    val logData = sc1.textFile(logFile, 3)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc1)

    val schemaString = "id name city"

    val rdd = logData.map(_.split(",")).map(p => Row(p(0), p(1), p(2)))

    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    //val customerDF = sqlContext.createDataFrame(rdd, schema);

    val customerDF = sqlContext.createDataFrame(rdd, schema)

    customerDF.createOrReplaceTempView("Subex_customers")
    sqlContext.cacheTable("Subex_customers")

    println(sqlContext.sql("select name from Subex_customers where city='London'").show())

    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    Thread.sleep(10000000)
  }
}