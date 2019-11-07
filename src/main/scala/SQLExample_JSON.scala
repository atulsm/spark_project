import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object SQLExample_JSON extends App {

  val logFile = "C:\\BigData\\Apache Spark\\TennisPlayers.JSON"

  val conf = new SparkConf().setAppName("Simple Application").
    setMaster("local[2]")
  val sc1 = new SparkContext(conf)
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc1)
  val df = sqlContext.read.json(logFile)

  // df.registerTempTable()
  df.createOrReplaceTempView("players_json")
  val result = sqlContext.sql("select id,details.grandslam from players_json where country='Swiss'")
  result.select("*").write.format("orc").save("C:\\orc3")
    
  
  //df.select("*").show

  Thread.sleep(20000888)
}