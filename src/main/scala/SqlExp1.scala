

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession

case class customers(id: Int, name: String, city: String)

object SqlExp1 {

  def main(args: Array[String]) {
    // val logFile = "file:////usr/hdp/2.4.0.0-169/spark/README.md" // Should be some file on your system set("spark.ui.port", "44040" ).

    val logFile = "/customers"

    val conf = new SparkConf().setAppName("SqlExample1")
    val sc1 = new SparkContext(conf)
    val logData = sc1.textFile(logFile, 3)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc1)

    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]")
      .config("spark.some.config.option", "some-value").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val customerDF = logData.map(row => row.split(',')).map(p => new customer(p(0).trim.toInt, p(1), p(2))).toDF()

    customerDF.createOrReplaceTempView("people")
    //customerDF.saveAsTable("Subex_customers")
    //println(sqlContext.sql("select name from Subex_customers where city='London'").show())

    val selectiveCustomerDF = spark.sql("SELECT name, id FROM people WHERE id BETWEEN 2 AND 4")

    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    selectiveCustomerDF.map(customer => "Name: " + customer(0)).show()
    selectiveCustomerDF.map(customer => "Name: " + customer.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    selectiveCustomerDF.map(customer => customer.getValuesMap[Any](List("name", "id"))).collect()

    selectiveCustomerDF.select("name", "id").write.format("parquet").save("/parquet_output")

    //val parquetFileDF = spark.read.parquet("C:\\namesAndAges.parquet")

    //parquetFileDF.createOrReplaceTempView("parquetFile")
    // val namesDF = spark.sql("SELECT name FROM parquetFile WHERE id BETWEEN 1 AND 3")

    Thread.sleep(10000000)
  }
}