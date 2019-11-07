package com.mukesh.spark.anatomy

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 27/3/15.
 */
object LazinessExample {

  def main(args: Array[String]) {

       
      val conf = new SparkConf()
    .setAppName("custom partition example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
      
   val dataRDD = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")    
  


    val flatMapRDD = dataRDD.flatMap(value => value.split(""))

    println("type of flatMapRDD is "+ flatMapRDD.getClass.getSimpleName +" and parent is " + flatMapRDD.dependencies.head.rdd.getClass.getSimpleName)

    val hadoopRDD = dataRDD.dependencies.head.rdd

    println("type of  Data RDD is " +dataRDD.getClass.getSimpleName+"  and the parent is "+ hadoopRDD.getClass.getSimpleName)

    println("parent of hadoopRDD is " + hadoopRDD.dependencies)


  }

}
