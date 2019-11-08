import java.util

import kafka.serializer.{ DefaultDecoder, StringDecoder }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ LongWritable, Writable, IntWritable, Text }
import org.apache.hadoop.mapred.{ TextOutputFormat, JobConf }
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{ HasOffsetRanges, KafkaUtils }
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark._

import scala.collection.mutable.ListBuffer

/**
 * Created by hkropp on 19/04/15.
 */
object SparkKafkaExample {

  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)

    val previousCount = state.getOrElse(0)

    Some(currentCount + previousCount)
  }

  def main(args: Array[String]): Unit =
    {
      if (args.length < 2) {
        System.err.println("Usage: <broker-list> <zk-list> <topic>")
        System.exit(1)
      }

      val Array(broker, zk, topic) = args

      val sparkConf = new SparkConf().setAppName("KafkaHBaseWordCount").setMaster("local[2]")
      val ssc = new StreamingContext(sparkConf, Seconds(1))
      ssc.checkpoint("checkpoints2") // checkpointing dir
      //ssc.checkpoint("hdfs://checkpoints")  // dir in hdfs for prod

      val kafkaConf = Map("metadata.broker.list" -> broker,
        "zookeeper.connect" -> zk,
        "group.id" -> "kafka-spark-streaming-example",
        "zookeeper.connection.timeout.ms" -> "1000")

      /* Kafka integration with reciever */
      //    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      //      ssc, kafkaConf, Map(topic -> 1),
      //      StorageLevel.MEMORY_ONLY_SER).map(_._2)

      val noOfstream = 5

      //    val lines = (1 to noOfstream).map(i => 
      //      KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      //      ssc, kafkaConf, Map(topic -> 1),
      //      StorageLevel.MEMORY_ONLY_SER).map(_._2))
      //      
      //    
      //    val unifiedLines = ssc.union(lines)

      //println(unifiedLines.foreachRDD(row => println(row)))

      /* Experiemental DirectStream w/o Reciever */
      val lines = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](
        ssc,
        kafkaConf,
        Set(topic)).map(_._2)

      /* Getting Kafka offsets from RDDs
    lines.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach( println(_) )
    }*/

      // val words = unifiedLines.flatMap(_.split(","))

      val words = lines.flatMap(_.split(","))

      //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
      // wordCounts.print()

      //  .reduceByKeyAndWindow(_ + _, _ - _, Minutes(5), Seconds(2), 2)

      // uncommment below code for stateful spark streaming

      val wordCounts = words.map(x => (x, 1))

      val windowedWordCounts = wordCounts.updateStateByKey(updateFunc)
      println(windowedWordCounts.print())

      ssc.start()
      ssc.awaitTermination()
    }

}