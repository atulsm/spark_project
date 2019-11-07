

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
object SparkLinearRegression extends App {

  val rootLogger = Logger.getLogger("org")
  rootLogger.setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)
  //val data = sc.textFile("C:\\BigData\\Apache Spark\\build-example\\example\\usb\\spark-training\\data\\examples-data\\kmeans_data.txt")

  val housingLines = sc.textFile("C:/BigData/Apache Spark/housing.data", 6)
  val housingVals = housingLines.map(x => Vectors.dense(x.split(",").map(_.trim().toDouble)))
  val housingData = housingVals.map(x => {
    val a = x.toArray;
    LabeledPoint(a(a.length - 1),
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

  val validPredicts = validScaled.map(x => (model.predict(x.features), x.label))
  //validPredicts.collect().foreach(println)
  val RMSE = math.sqrt(validPredicts.map { case (p, l) => math.pow(p - l, 2) }.mean())

  val validMetrics = new RegressionMetrics(validPredicts)
  println(validMetrics.rootMeanSquaredError)
  println(validMetrics.meanSquaredError)
  //println(model.weights.toArray.map(x => x.abs).zipWithIndex.sortBy(_._1).mkString(", "))

  //
  //model.save(sc, "hdfs:///path/to/saved/model")

  //val model_load = LinearRegressionModel.load(sc, "hdfs:///path/to/saved/model")
  Thread.sleep(55555555);

}