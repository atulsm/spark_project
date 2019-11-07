
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object HdfsApp {
  def main(args: Array[String]) {
    val logFile = args(0) // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Hdfs Application").set("spark.driver.allowMultipleContexts", "true")
    val sc1 = new SparkContext(conf)
    val logData = sc1.textFile(logFile, 2).cache()
    val lineLengths = logData.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    lineLengths.persist()
    println("total length is")
    println(totalLength)
  }
}