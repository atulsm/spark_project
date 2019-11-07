

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
object SparkLinearRegSGDOptimizationLBFGS extends App {

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

  iterateLBFGS(Array(0.005, 0.007, 0.01, 0.02, 0.03, 0.05, 0.1), 10, 1e-5, trainHPScaled, validHPScaled)

  def addHighPols(v: Vector): Vector =
    {
      Vectors.dense(v.toArray.flatMap(x => Array(x, x * x)))
    }

  def iterateLBFGS(regParams: Array[Double], numCorrections: Int, tolerance: Double, train: RDD[LabeledPoint], test: RDD[LabeledPoint]) = {
    import org.apache.spark.mllib.optimization.LeastSquaresGradient
    import org.apache.spark.mllib.optimization.SquaredL2Updater
    import org.apache.spark.mllib.optimization.LBFGS
    import org.apache.spark.mllib.util.MLUtils
    val dimnum = train.first().features.size
    for (regParam <- regParams) {
      val (weights: Vector, loss: Array[Double]) = LBFGS.runLBFGS(
        train.map(x => (x.label, MLUtils.appendBias(x.features))),
        new LeastSquaresGradient(),
        new SquaredL2Updater(),
        numCorrections,
        tolerance,
        50000,
        regParam,
        Vectors.zeros(dimnum + 1))

      val model = new LinearRegressionModel(
        Vectors.dense(weights.toArray.slice(0, weights.size - 1)),
        weights(weights.size - 1))

      val trainPredicts = train.map(x => (model.predict(x.features), x.label))
      val validPredicts = test.map(x => (model.predict(x.features), x.label))
      val meanSquared = math.sqrt(trainPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      val meanSquaredValid = math.sqrt(validPredicts.map({ case (p, l) => math.pow(p - l, 2) }).mean())
      println("%5.3f, %d -> %.4f, %.4f".format(regParam, numCorrections, meanSquared, meanSquaredValid))
    }
  }

}

