import org.apache.spark.sql.SparkSession

object SQL1_New_DS {
  case class TenisPlayer(name: String, country: String, id: Long)
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[2]")
      .config("spark.some.config.option", "some-value").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val caseClassDS = Seq(TenisPlayer("Sania", "India", 10)).toDS()
    caseClassDS.show()

    //val df = spark.read.json("C:\\BigData\\Apache Spark\\TennisPlayers.JSON")

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "C:\\BigData\\Apache Spark\\TennisPlayers.JSON"
    val peopleDS = spark.read.json(path).as[TenisPlayer]
    peopleDS.show()
    // Displays the content of the DataFrame to stdout

  }

}