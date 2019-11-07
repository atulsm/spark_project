package com.mukesh.spark.apiexamples.serilization

/**
 * Created by mukesh on 20/1/15.
 */
object SalesRecordParser {
  def parse(record:String): Either[MalformedRecordException,SalesRecord] = {
    val values: Array[String] = record.split(",")
    if (values.length < 4) return Left(new MalformedRecordException())
    else {
      val transactionId: String = values(0)
      val customerId: String = values(1)
      val itemId: String = values(2)
      val itemValue: Double = values(3).toDouble
      return Right(new SalesRecord(transactionId, customerId, itemId, itemValue))
    }
  }

}
