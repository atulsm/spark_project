package com.mukesh.spark.anatomy

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 11/3/15.
 */
object HashPartitioningExample {

  def main(args: Array[String]) {    

   val conf = new SparkConf()
    .setAppName("hash partitioning example")
    .setMaster("local[4]")
    
    val sc = new SparkContext(conf)
    

    val salesData = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(3).toDouble)
    })

    val groupedData = salesByCustomer.groupByKey()

    println("default partitions are "+groupedData.partitions.length)


    //increase partitions
    val groupedDataWithMultiplePartition = salesByCustomer.groupByKey(3)
    println("increased partitions are " + groupedDataWithMultiplePartition.partitions.length)

  }

}
