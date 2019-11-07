package com.mukesh.spark.apiexamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext._


/**
 * Created by mukesh on 20/1/15.
 */
object ItemWiseCount {
  def main(args: Array[String]) {

    //val sc = new SparkContext(args(0), "apiexamples")
   // val dataRDD = sc.textFile(args(1))
    
    val conf = new SparkConf()
    .setAppName("apiexamples example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    

    val dataRDD = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")
    
    val itemPair = dataRDD.map(row => {
      val columns = row.split(",")
      (columns(2), 1)
    })
    /*
    itemPair is MappedRDD which is a pair. We can import the following to get more methods

     */
   
    val result = itemPair.reduceByKey(_+_)

    println(result.collect().toList)

  }
}
