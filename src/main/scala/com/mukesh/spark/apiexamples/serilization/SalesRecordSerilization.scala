package com.mukesh.spark.apiexamples.serilization

import org.apache.spark.SparkContext

/**
 * Created by mukesh on 20/1/15.
 */
object SalesRecordSerilization {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    println(salesRecordRDD.collect().toList)
  }

}
