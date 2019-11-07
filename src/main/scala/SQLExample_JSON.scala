import org.apache.spark.sql.SparkSession

object SQLExample_JSON extends App {

  val logFile = "/tmp/TennisPlayers.JSON"

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val df = spark.read.json(logFile)
  df.show

  // df.registerTempTable()
  df.createOrReplaceTempView("players_json")
  val result = spark.sql("select id,details.grandslam from players_json where country='Swiss'")
  result.select("*").write.format("orc").save("/tmp/orc3")

  df.select("*").show

  Thread.sleep(20000888)
}