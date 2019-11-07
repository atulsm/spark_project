package com.mukesh.spark.apiexamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 20/1/15.
 */
object LoadData {

  def main(args: Array[String]) {
    //create spark context
    val sc = new SparkContext(args(0),"apiexamples")
    
    val conf = new SparkConf()
    .setAppName("apiexamples example")
    .setMaster("local[2]")
    
    //val sc = new SparkContext(conf)
    

    val dataRDD = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")

    //it creates RDD[String] of type MappedRDD

    //val dataRDD = sc.textFile(args(1))

    //print the content . Converting to List just for nice formatting
    println(dataRDD.collect().toList)
  }


}
