package com.mukesh.spark.apiexamples.joins

import java.io.{File, FileReader, BufferedReader}
import com.mukesh.spark.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable
import org.apache.spark.SparkConf

/**
 * Created by mukesh on 20/1/15.
 */
object BroadcastBased {


  def parseCustomerData(customerDataPath: String): mutable.Map[String, String] = {

    val customerMap = mutable.Map[String, String]()

    val bufferedReader = new BufferedReader(new FileReader(new File(customerDataPath)))
    var line: String = null
    while ((({
      line = bufferedReader.readLine;
      line
    })) != null) {
      val values = line.split(",")
      customerMap.put(values(0), values(1))
    }
    bufferedReader.close()
    customerMap
  }


  def main(args: Array[String]) {

    val conf = new SparkConf()
    .setAppName("apiexamples example")
    .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val salesRDD = sc.textFile(args(0))
    val customerDataPath = args(1)
    val customerMap = parseCustomerData(customerDataPath)

    //broadcast data

    val customerBroadCast = sc.broadcast(customerMap)

    val joinRDD = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      val customerName = customerBroadCast.value(salesRecord.customerId)
      (customerName,salesRecord)
    })


    println(joinRDD.collect().toList)
     Thread.sleep(444444444)

  }


}
