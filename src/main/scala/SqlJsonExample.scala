

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SqlJsonExample {
  def main(args: Array[String]): Unit = {
    val logFile= "C:\\BigData\\Apache Spark\\build-example\\TennisPlayers.json"
    
    val conf = new SparkConf().setAppName("SqlExample1").setMaster("local[2]") 
    val sc1 = new SparkContext(conf)
    
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc1)
    val df = sqlContext.read.json(logFile)
    
    df.createOrReplaceTempView("tennis_top_players")
    sqlContext.sql("select Name,details.grandslam,country from tennis_top_players").show
    
  }
}