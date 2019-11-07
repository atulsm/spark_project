package com.mukesh.spark.apiexamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Created by mukesh on 20/1/15.
 */
object ItemFilter {
   val itemIDToSearch = 1002
  def main(args: Array[String]) {

    //val sc = new SparkContext(args(0), "apiexamples")
    //val dataRDD = sc.textFile(args(1))
   
    
    val conf = new SparkConf()
    .setAppName("API example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    
    

    val dataRDD = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")

    dataRDD.checkpoint
    
    val itemRows =  dataRDD.filter(itemFilter).checkpoint
    
   

    //println(itemRows.collect().toList)
    
    dataRDD.map(item => (item.split(",")(2),item.split(",")(3).toInt)).
    reduceByKey(_+_,2).collect().foreach(println)
    
    Thread.sleep(700000000);
    
  }
  
   def itemFilter(row:String): Boolean = {
      val columns = row.split(",")
      val itemId = columns(2).toInt
      if(itemId == (itemIDToSearch)) true
      else false
    }
}
