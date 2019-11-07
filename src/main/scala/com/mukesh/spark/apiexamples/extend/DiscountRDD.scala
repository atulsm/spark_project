package com.mukesh.spark.apiexamples.extend

import com.mukesh.spark.apiexamples.serilization.SalesRecord
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
 * Created by mukesh on 27/2/15.
 */
class DiscountRDD(prev:RDD[SalesRecord],discountPercentage:Double) extends RDD[SalesRecord](prev){
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] =  {
    firstParent[SalesRecord].iterator(split, context).map(salesRecord => {
      val discount = salesRecord.itemValue*discountPercentage
      new SalesRecord(salesRecord.transactionId,salesRecord.customerId,salesRecord.itemId,discount)
    })}

  override protected def getPartitions: Array[Partition] = firstParent[SalesRecord].partitions
}
