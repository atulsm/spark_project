package com.mukesh.spark.anatomy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder

case class Stock_MaxClosePrice(stock: String, maxPrice: Float)

case class Stock_trading_data(exchange: String, stock: String, date: String,
                              stock_price_open: Float, stock_price_high: Float, stock_price_low: Float,
                              stock_price_close: Float,
                              stock_volume: Long, stock_price_adj_close: Float)

object MaxPriceDetectorKafka extends App {
  if (args.length < 2) {
    System.err.println("Usage: <broker-list> <zk-list> <topic>")
    System.exit(1)
  }

  val Array(broker, zk, topic) = args

  val custFile = "C:\\stock_max"

  val conf = new SparkConf().setAppName("Demo_Assignment_Kafka").setMaster("local[4]")

  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc);

  import sqlContext.implicits._

  val stockMaxDF = sc.textFile(custFile).map(line => line.split(",")).map(row =>
    new Stock_MaxClosePrice(row(0), row(1).toFloat)).toDF()
  stockMaxDF.createOrReplaceTempView("StockMaxPrice")
  
  stockMaxDF.cache()

  //stockMaxDF.show
  val ssc = new StreamingContext(sc, Seconds(10))

  val kafkaConf = Map("metadata.broker.list" -> broker,
    "zookeeper.connect" -> zk,
    "group.id" -> "kafka-spark-streaming-example",
    "zookeeper.connection.timeout.ms" -> "1000")

  val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
    ssc, kafkaConf, Map(topic -> 1),
    StorageLevel.MEMORY_ONLY_SER).map(_._2)

  lines.foreachRDD { data =>
    val sqlContext1 = SQLContext.getOrCreate(data.sparkContext)

    val stockDataFrame = data.map(line => line.split(',')).map(row => new Stock_trading_data(row(0),
      row(1), row(2), row(3).toFloat, row(4).toFloat, row(5).toFloat,
      row(6).toFloat, row(7).toLong, row(8).toFloat)).toDF()

    stockDataFrame.createOrReplaceTempView("StockTradingData")    
   
    val flt=  sqlContext.sql("Select A.stock,CASE when ROUND(A.maxPrice - B.stock_price_close,2) < 0 THEN \"NewMax\" else \"NoMax\" END AS Status from StockMaxPrice A JOIN StockTradingData B ON A.stock=B.stock")
  
    
    if (flt.count>0) {
      println("Max Price Detection, please find stock name and its status")
      flt.show
      
    } 

  }

  ssc.start()
  ssc.awaitTermination()

}