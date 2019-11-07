package com.mukesh.spark.hadoopintegration.read

import com.mukesh.spark.apiexamples.serilization.SalesRecord
import com.mukesh.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext

/**
 * Created by mukesh on 20/1/15.
 */
object ObjectFileRead {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"apiexamples")
    val dataRDD = sc.objectFile[SalesRecord](args(1))
    println(dataRDD.collect().toList)

  }

}
