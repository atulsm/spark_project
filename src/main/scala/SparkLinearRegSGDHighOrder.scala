

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
object SparkLinearRegSGDHighOrder extends App {

  val rootLogger = Logger.getLogger("org")
  rootLogger.setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)
  //val data = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\example\\usb\\spark-training\\data\\examples-data\\kmeans_data.txt")

  val housingLines = sc.textFile("C:/BigData/Apache Spark/housing.data", 6)
  val housingVals = housingLines.map(x => Vectors.dense(x.split(",").map(_.trim().toDouble)))
  val housingData = housingVals.map(x => {
    val a = x.toArray; LabeledPoint(a(a.length - 1),
      Vectors.dense(a.slice(0, a.length - 1)))
  })

  
  val housingHP = housingData.map(v => LabeledPoint(v.label, addHighPols(v.features)))

  housingHP.first().features.size

  val setsHP = housingHP.randomSplit(Array(0.8, 0.2))
  val housingHPTrain = setsHP(0)
  val housingHPValid = setsHP(1)
  val scalerHP = new StandardScaler(true, true).fit(housingHPTrain.map(x => x.features))
  val trainHPScaled = housingHPTrain.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
  val validHPScaled = housingHPValid.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
  trainHPScaled.cache()
  validHPScaled.cache()

  iterateLRwSGD(Array(200, 400), Array(0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5), trainHPScaled, validHPScaled)

  
  
  //iterateLRwSGD(Array(200, 400, 800, 1000, 3000, 6000), Array(1.1), trainHPScaled, validHPScaled)
  
  //iterateLRwSGD(Array(10000, 15000, 30000, 50000), Array(1.1), trainHPScaled, validHPScaled)
  
  def iterateLRwSGD(iterNums: Array[Int], stepSizes: Array[Double], train: RDD[LabeledPoint], test: RDD[LabeledPoint]) = {
    for (numIter <- iterNums; step <- stepSizes) {
      val alg = new LinearRegressionWithSGD()
      alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
      val model = alg.run(train)
      val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
      val validPredicts = test.map(x => (model.predict(x.features), x.label))
      //validPredicts.collect.foreach(println)
      val meanSquared = math.sqrt(rescaledPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      val meanSquaredValid = math.sqrt(validPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
      //Uncomment if you wish to see weghts and intercept values:
      //println("%d, %4.2f -> %.4f, %.4f (%s, %f)".format(numIter, step, meanSquared, meanSquaredValid, model.weights, model.intercept))
    }

  }
  def addHighPols(v: Vector): Vector =
    {
      Vectors.dense(v.toArray.flatMap(x => Array(x, x * x )))
    }

}