package com.mukesh.spark.apiexamples.errorhandling

import com.mukesh.spark.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 20/1/15.
 */
object Counters {
  def main(args: Array[String]) {

   // val sc = new SparkContext(args(0), "apiexamples")
   // val dataRDD = sc.textFile(args(1))
    
    val conf = new SparkConf()
    .setAppName("apiexamples example")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    

    val dataRDD = sc.textFile("/tmp/sales.txt",3)
    val malformedRecords = sc.longAccumulator("acc0")

    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      if(parseResult.isLeft){
        malformedRecords.add(1)
      }
      parseResult
      malformedRecords.add(10)
    })


    //run collect
    salesRecordRDD.collect()

    //print the counter

    //println("No of malformed records is =  " + malformedRecords.value)
    Thread.sleep(444444444)


  }

}
