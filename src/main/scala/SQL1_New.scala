import org.apache.spark.sql.SparkSession

object SQL1_New {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]")
      .config("spark.some.config.option", "some-value").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("C:\\BigData\\Apache Spark\\TennisPlayers.JSON")

    // Displays the content of the DataFrame to stdout
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"id" + 1).show()
    val filtdata= df.filter($"id" > 2)

    df.createOrReplaceTempView("people")
    
    filtdata.createOrReplaceTempView("people1")
    
    val sqlDF1 = spark.sql("SELECT * FROM people1")
    sqlDF1.show()

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    df.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    df.show()
    Thread.sleep(5555558)
  }

}