
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object Ml_kmeans extends App {
 
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]") 
  val sc = new SparkContext(conf)
  val data = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\example\\usb\\spark-training\\data\\examples-data\\kmeans_data.txt")
  val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
  
  // Cluster the data into two classes using KMeans
  val numClusters = 2
  val numIterations = 20
  val clusters = KMeans.train(parsedData, numClusters, numIterations)
  
  // Evaluate clustering by computing Within Set Sum of Squared Errors
  val WSSSE = clusters.computeCost(parsedData)
  println("Within Set Sum of Squared Errors = " + WSSSE)

  
}