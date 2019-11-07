package com.mukesh.spark.anatomy

import org.apache.spark.Partitioner

/**
 * Created by mukesh on 11/3/15.
 */
class CustomerPartitioner extends Partitioner {
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int =
    key match {
      case null => 0
      case _ => {
        val keyValue = key.toString.toInt
        if(keyValue>=100) 1 
        else if (keyValue < 3) 2
        else 0
      }
    }
}
