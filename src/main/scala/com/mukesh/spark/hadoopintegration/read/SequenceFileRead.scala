package com.mukesh.spark.hadoopintegration.read

import com.mukesh.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext

/**
 * Created by mukesh on 20/1/15.
 */
object SequenceFileRead {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"apiexamples")
    val dataRDD = sc.sequenceFile(args(1),classOf[NullWritable],classOf[SalesRecordWritable]).map(_._2)
    println(dataRDD.collect().toList)

  }

}
