

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
object SparkLinearRegSGDOptimization extends App {

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

  val sets = housingData.randomSplit(Array(0.8, 0.2))
  val housingTrain = sets(0)
  val housingValid = sets(1)

  val scaler = new StandardScaler(true, true).fit(housingTrain.map(x => x.features))
  val trainScaled = housingTrain.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
  val validScaled = housingValid.map(x => LabeledPoint(x.label, scaler.transform(x.features)))

  val alg = new LinearRegressionWithSGD()
  alg.setIntercept(true)
  alg.optimizer.setNumIterations(200)
  trainScaled.cache()
  validScaled.cache()
  val model = alg.run(trainScaled)

  //
  //model.save(sc, "hdfs:///path/to/saved/model")

  //val model_load = LinearRegressionModel.load(sc, "hdfs:///path/to/saved/model")

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

  //iterateLRwSGDBatch(Array(400, 1000), Array(0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1), Array(0.01, 0.1), trainHPScaled, validHPScaled)

  iterateLRwSGDBatch(Array(400, 1000, 2000, 3000, 5000, 10000), Array(0.4), Array(0.1, 0.2, 0.4, 0.5, 0.6, 0.8), trainHPScaled, validHPScaled)

  def addHighPols(v: Vector): Vector =
    {
      Vectors.dense(v.toArray.flatMap(x => Array(x, x * x)))
    }

  def iterateRidge(iterNums: Array[Int], stepSizes: Array[Double], regParam: Double, train: RDD[LabeledPoint], test: RDD[LabeledPoint]) = {
    import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
    for (numIter <- iterNums; step <- stepSizes) {
      val alg = new RidgeRegressionWithSGD()
      alg.setIntercept(true)
      alg.optimizer.setNumIterations(numIter).setRegParam(regParam).setStepSize(step)
      val model = alg.run(train)
      val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
      val validPredicts = test.map(x => (model.predict(x.features), x.label))
      val meanSquared = math.sqrt(rescaledPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      val meanSquaredValid = math.sqrt(validPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
    }
  }

  def iterateLRwSGDBatch(iterNums: Array[Int], stepSizes: Array[Double], fractions: Array[Double], train: RDD[LabeledPoint], test: RDD[LabeledPoint]) = {
    for (numIter <- iterNums; step <- stepSizes; miniBFraction <- fractions) {
      val alg = new LinearRegressionWithSGD()
      alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
      alg.optimizer.setMiniBatchFraction(miniBFraction)
      val model = alg.run(train)
      val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
      val validPredicts = test.map(x => (model.predict(x.features), x.label))
      val meanSquared = math.sqrt(rescaledPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      val meanSquaredValid = math.sqrt(validPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      println("%d, %5.3f %5.3f -> %.4f, %.4f".format(numIter, step, miniBFraction, meanSquared, meanSquaredValid))
    }

  }

}

