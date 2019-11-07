package com.mukesh.spark.apiexamples.serilization

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by mukesh on 20/1/15.
 */
object SalesRecordKryoSerilization {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[SalesRecord]))
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    println(salesRecordRDD.collect().toList)
  }

}
