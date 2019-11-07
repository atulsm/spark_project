package com.mukesh.spark.anatomy
import org.apache.spark.Partitioner

/**
 * Created by mukesh on 09/23/17.
 */
class CustomPartitionerByClosePrice extends Partitioner {
  override def numPartitions: Int = 6

  override def getPartition(symbol: Any): Int =
    symbol match {
      case null => 0
      case _ => {
        val firstChar = symbol.toString().charAt(0)
        
        (firstChar - 'A') % numPartitions
        
      }
    }
}
