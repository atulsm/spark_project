package com.mukesh.spark.apiexamples.discount

import com.mukesh.spark.apiexamples.serilization.{SalesRecord, SalesRecordParser}
import org.apache.spark.SparkContext
import org.apache.spark._

/**
 * Created by mukesh on 20/1/15.
 */
object AmountWiseDiscount {

  def main(args: Array[String]) {
    //val sc = new SparkContext(args(0), "apiexamples")
    //val dataRDD = sc.textFile(args(1))
    
    val conf = new SparkConf()
    .setAppName("apiexamples example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    

    val dataRDD = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val totalAmountByCustomer = salesRecordRDD.map(row => (row.customerId, row.itemValue)).reduceByKey(_ + _,4)

    val discountAmountByCustomer = totalAmountByCustomer.map {
      case (customerId, totalAmount) => {
        if (totalAmount > 500) {
          val afterDiscount = totalAmount - (totalAmount * 10) / 100.0
          (customerId, afterDiscount)
        }
        else (customerId, totalAmount)
      }
    }

    println(discountAmountByCustomer.saveAsTextFile("/spark_sales_discount"))
    Thread.sleep(2222222)


  }
}

