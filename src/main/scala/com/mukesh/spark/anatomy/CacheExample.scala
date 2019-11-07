package com.mukesh.spark.anatomy

import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{SparkContext, SparkEnv,SparkConf}

/**
 * Created by mukesh on 24/3/15.
 */
object CacheExample {

  def main(args: Array[String]) {
    
    val conf = new SparkConf()
    .setAppName("cache example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    

    val salesData = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt",2)    

    val salesByCustomer = salesData.map(value => {
      println("computed")
      val colValues = value.split(",")
      (colValues(1), colValues(3).toDouble)
    })

    println("salesData rdd id is " + salesData.id)
    println("salesBy customer id is " + salesByCustomer.id)



    val firstPartition = salesData.partitions.head   
  

    salesData.cache()

    println(" the persisted RDD's " + sc.getPersistentRDDs)

  }


}
