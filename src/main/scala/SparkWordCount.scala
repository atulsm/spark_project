

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkWordCount {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName(" My first spark Application").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val logRdd= sc.textFile("/input")
    val result  = logRdd.flatMap( textline => textline.split(" ")).map(word => (word,1)).reduceByKey(_+_,4)
    
    result.saveAsTextFile("/spark_wc_out")
    Thread.sleep(100000)
  }
  
}