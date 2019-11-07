package com.mukesh.spark.hadoopintegration.write

import org.apache.spark.SparkContext

/**
 * Created by mukesh on 20/1/15.
 */
object TextFilePersist {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val outputPath = args(2)
    val itemPair = dataRDD.map(row => {
      val columns = row.split(",")
      (columns(2), 1)
    })
    /*
    itemPair is MappedRDD which is a pair. We can import the following to get more methods

     */
    import org.apache.spark.SparkContext._

    val result = itemPair.reduceByKey(_+_)

    result.saveAsTextFile(outputPath)


  }

}
