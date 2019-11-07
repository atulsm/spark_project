package com.mukesh.spark.anatomy

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import java.lang.Math
/**
 * Created by mukesh on 09/25/2017
 */
object CustomPartitionExampleByPrice {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("custom partitioning example")
      .setMaster(args(0))

    val sc = new SparkContext(conf)

    val stockData = sc.textFile(args(1))
    //val dividendData = sc.textFile(args(2))
    
    // Following will run 2 instances of map task 

    val closePriceBySymbol = stockData.map(value => {
      val colValues = value.split(",")
      (colValues(1), colValues(6).toFloat)
    })
    
    // We are going to perform 10 tasks of reduceyByKey by passing 10 as the 2nd argument in the reduceByKey method

    val maxPriceSymbol = closePriceBySymbol.reduceByKey((a, b) => a max b,10).
      map({ case (stock, amount) => (amount, stock) }).sortByKey(false).
      map({ case (amount, stock) => (stock, amount) })

   // use HDFS path if you want to save it to HDFS directory such as hdfs://Namenode:8020/stock_min_max
   maxPriceSymbol.map { x => x.productIterator.toSeq.mkString(",") }.saveAsTextFile("C:\\stock_max")
  
    val stockGroupedData = maxPriceSymbol.groupByKey(new CustomPartitionerByClosePrice)
    
   
    println(stockGroupedData.partitions.length)
   // val (min,max)
      val minMaxForPartition= stockGroupedData.mapPartitions(iterator => {
      val (min,max) = iterator.foldLeft((Float.MaxValue,Float.MinValue))((acc,salesRecord) => {
        (acc._1 min salesRecord._2.sum, acc._2 max salesRecord._2.sum)
      })
      List((min,max)).iterator
     
    })
    
    minMaxForPartition.saveAsTextFile("C:\\partition_output")
    
    val (min,max) = minMaxForPartition.reduce((a,b)=> (a._1 min b._1 , a._2 max b._2))  
    
     println("min = "+min + " max ="+max)
    

   

  }

}
