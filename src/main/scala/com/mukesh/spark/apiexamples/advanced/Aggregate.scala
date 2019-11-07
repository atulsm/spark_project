package com.mukesh.spark.apiexamples.advanced

import com.mukesh.spark.apiexamples.serilization.{SalesRecord, SalesRecordParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 28/1/15.
 */
object Aggregate {
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

    val zeroValue = (Double.MaxValue,0.0)

    val seqOp = ( minMax:(Double,Double), record: SalesRecord) => {
        val currentMin = minMax._1
        val currentMax = minMax._2
        val min = if(currentMin > record.itemValue) record.itemValue else currentMin
        val max = if(currentMax < record.itemValue) record.itemValue else currentMax
        (min,max)
      }

    val combineOp = (firstMinMax:(Double,Double), secondMinMax:(Double,Double)) => {
      ((firstMinMax._1 min secondMinMax._1), (firstMinMax._2 max secondMinMax._2))
     }

    val minmax = salesRecordRDD.aggregate(zeroValue)(seqOp,combineOp)

    println("mi = "+ minmax._1 +" and max = "+ minmax._2)


  }





}
