

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, BlockMatrix, DistributedMatrix, MatrixEntry}
import breeze.linalg.{DenseMatrix => BDM,CSCMatrix => BSM,Matrix => BM}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MLInAction extends App {

  val rootLogger = Logger.getLogger("org")
  rootLogger.setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)
  //val data = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\example\\usb\\spark-training\\data\\examples-data\\kmeans_data.txt")

  val housingLines = sc.textFile("C:/BigData/Apache Spark/housing.data", 6)
  val housingVals = housingLines.map(x => Vectors.dense(x.split(",").map(_.trim().toDouble)))
  val housingMat = new RowMatrix(housingVals)
  val housingStats = housingMat.computeColumnSummaryStatistics()
  val housingColSims = housingMat.columnSimilarities()

  
  
  MlUtility.printMat(MlUtility.toBreezeD(housingColSims))
  val housingCovar = housingMat.computeCovariance()
  MlUtility.printMat(MlUtility.toBreezeM(housingCovar))
  

}