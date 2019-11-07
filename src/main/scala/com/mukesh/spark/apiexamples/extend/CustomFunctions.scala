package com.mukesh.spark.apiexamples.extend

import com.mukesh.spark.apiexamples.extend.DiscountRDD
import com.mukesh.spark.apiexamples.serilization.SalesRecord
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by mukesh on 27/2/15.
 */
class CustomFunctions(rdd:RDD[SalesRecord]) {

  def totalAmount = rdd.map(_.itemValue).sum
  
  def discount(discountPercentage:Double) = new DiscountRDD(rdd,discountPercentage)

}

object CustomFunctions {

  implicit def toUtils(rdd: RDD[SalesRecord]) = new CustomFunctions(rdd)
}
