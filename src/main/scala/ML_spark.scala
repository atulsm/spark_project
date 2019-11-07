
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ML_spark extends App {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)
  //val data = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\example\\usb\\spark-training\\data\\examples-data\\kmeans_data.txt")

  /*A certain real estate firm wants to build homes targeting different buyers. The only information that firm has collected is the income and the
current property locations of their potential buyers. The firm wants to group their potential buyers into some group which is not obvious by dataset itself.

Let us solve this case by using K Means Clustering algorithm offered by MLLib of Apache Spark.

This is how our dataset looks like:

1
2
3
4
buyerId,longitude,latitude,income
1,111.3717,-6.7058,55238
2,15.43647,12.47554,863961
3,23.01361,56.7325,719865 */

  val rawData = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\Sales.txt")
  val extractedFeatureVector = rawData.map(row => Vectors.dense(row.split(',').slice(1, 5).map(_.toDouble))).cache

  println(extractedFeatureVector.foreach(println))

  val numberOfClusters = 3
  val numberOfInterations = 50

  val model = KMeans.train(extractedFeatureVector, numberOfClusters, numberOfInterations)
  model.clusterCenters.foreach(println)

  val buyersByCluster = rawData.map(
    row =>
      (model.predict(Vectors.dense(row.split(',').slice(1, 5).map(_.toDouble))), row.split(',').slice(0, 1).head))
  println(buyersByCluster.foreach(println))
}