package com.mukesh.spark.apiexamples.advanced

import com.mukesh.spark.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext

/**
 * Created by mukesh on 28/1/15.
 */
object MapPartitions {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    //find max and min sale
    val (min,max) = salesRecordRDD.mapPartitions(iterator => {
      val (min,max) = iterator.foldLeft((Double.MaxValue,Double.MinValue))((acc,salesRecord) => {
        (acc._1 min salesRecord.itemValue , acc._2 max salesRecord.itemValue)
      })
      List((min,max)).iterator
    }).reduce((a,b)=> (a._1 min b._1 , a._2 max b._2))
    println("min = "+min + " max ="+max)
  }
}
