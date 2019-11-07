package com.mukesh.spark.anatomy

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 11/3/15.
 */
object CustomPartitionExample {


  
  def main(args: Array[String]) {
    
    val conf = new SparkConf()
    .setAppName("custom partitioning example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    

    val salesData = sc.textFile(args(0))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(3).toDouble)
    })

    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner)

    //printing partition specific data

    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex{
      case(partitionNo,iterator) => {
       List((partitionNo,iterator.toList.length)).iterator
      }
    }

    println(groupedDataWithPartitionData.collect().toList)
    
    Thread.sleep(7000000);
    
    
    
  }
  

}
