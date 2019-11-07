package com.mukesh.spark.anatomy

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 11/3/15.
 */
object PartitionExample {

  def main(args: Array[String]) {

   
    
    val conf = new SparkConf()
    .setAppName("partition example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    

    //val salesData = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")

    //actual textFile api converts to the following code
    val dataRDD = sc.hadoopFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      sc.defaultMinPartitions).map(pair => pair._2.toString)
    println(dataRDD.partitions.length)

  }


}
