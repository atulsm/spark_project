package com.mukesh.spark.apiexamples.extend

import com.mukesh.spark.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext

/**
 * Created by mukesh on 27/2/15.
 */
object CustomFunctionExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    import com.mukesh.spark.apiexamples.extend.CustomFunctions._
    salesRecordRDD.totalAmount
  }

}
