package com.mukesh.spark.apiexamples.joins

import com.mukesh.spark.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by mukesh on 20/1/15.
 */
object ShuffleBased {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "apiexamples")
    val salesRDD = sc.textFile(args(1))
    val customerRDD = sc.textFile(args(2))

    val salesPair = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      (salesRecord.customerId,salesRecord)
    })

    val customerPair = customerRDD.map(row => {
      val columnValues = row.split(",")
      (columnValues(0),columnValues(1))
    })


    val joinRDD = customerPair.join(salesPair,6).map{
      case (customerId,(customerName,salesRecord)) => {
        (customerName,salesRecord)
      }
    }

    println(joinRDD.collect().toList)
    
    Thread.sleep(444444444)

  }


}
