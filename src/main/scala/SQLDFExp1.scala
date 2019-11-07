

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Customer10(id: Int, name: String, city: String)

case class Sale(serial: Int, cust_id: Int, product_id: Int, amount: Long)

object SQLDFExp1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(" My first spark Application").setMaster("local[3]")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.setConf("spark.sql.json.compression.codec", "snappy")

    import sqlContext.implicits._

    val custRDD = sc.textFile("C:\\BigData\\Apache Spark\\Customers.csv").map(row => row.split(","))
      .map(record => Customer10(record(0).toInt, record(1), record(2))).toDF()

    val saleRDD = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt").map(row => row.split(","))
      .map(record => Sale(record(0).toInt, record(1).toInt, record(2).toInt, record(2).toLong)).toDF()

    custRDD.createOrReplaceTempView("cisco_customer")
    //custRDD.registerTempTable("cisco_customer")

    saleRDD.createOrReplaceTempView("cisco_sales")

    sqlContext.cacheTable("cisco_customer")

    sqlContext.cacheTable("cisco_sales")

    sqlContext.sql("select A.name,SUM(B.amount) as total from cisco_customer A join cisco_sales B On A.id=B.cust_id group by A.name order by total desc limit 3").show

    var result = sqlContext.sql("select cust_id,SUM(amount) As total from cisco_sales group by cust_id")
    result.write.format("json").save("C:\\avro1")

  }

}