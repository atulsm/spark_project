import java.util.Properties

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import twitter4j.TwitterFactory
import twitter4j.conf.ConfigurationBuilder

/**
 * Created by madhu on 3/7/14.
 */
object TwitterStream {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(args(0), "Twittercount", Seconds(10))
    val hashTag = args(1)
    val cb = new ConfigurationBuilder()
    val properties = new Properties()
    properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("twitter.properties"))
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(properties.getProperty("auth_consumer_key"))
      .setOAuthConsumerSecret(properties.getProperty("auth_consumer_secret"))
      .setOAuthAccessToken(properties.getProperty("auth_access_token"))
      .setOAuthAccessTokenSecret(properties.getProperty("auth_access_token_secret"))

    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()
    val auth = twitter.getAuthorization

    val filters = Array(hashTag)
    val twitterStream = TwitterUtils.createStream(ssc, Some(auth), filters)

    //caching enabled

    twitterStream.cache()

    val lines = twitterStream.map(status => status.getText)
    lines.print()
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    //wordCounts.count()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}