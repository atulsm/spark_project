package com.mukesh.spark.anatomy

import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.log4j.{Level,Logger}



/**
 * Created by mukesh on 11/3/15.
 */
object LookUpExample {

  def main(args: Array[String]) {  
    
    val rootLogger= Logger.getRootLogger()

    rootLogger.setLevel(Level.ERROR)
    
     val conf = new SparkConf()
    .setAppName("look up example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    

    val salesData = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1), colValues(3).toDouble)
    })


    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner)

    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex((partitionNo, iterator) => {
      println(" accessed partition " + partitionNo)
      iterator
    }, true)


    println("for accessing customer id 1")
    groupedDataWithPartitionData.lookup("1")


  }


}
