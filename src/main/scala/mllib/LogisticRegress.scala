package mllib

object LogisticRegress extends App {
  import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

val conf = new SparkConf().setAppName("Simple Application").set("spark.driver.allowMultipleContexts", "true").setMaster("local[2]") 
val sc = new SparkContext(conf)
val data = MLUtils.loadLibSVMFile(sc, "C:\\BigData\\Apache Spark\\Spark_code-exmaple\\java\\spark\\data\\mllib\\sample_libsvm_data.txt")

// Split data into training (60%) and test (40%).
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val model = new LogisticRegressionWithLBFGS()
  .setNumClasses(10)
  .run(training)

// Compute raw scores on the test set.
val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
  val prediction = model.predict(features)
  (prediction, label)
}

// Get evaluation metrics.
val metrics = new MulticlassMetrics(predictionAndLabels)
val precision = metrics.precision
println("Precision = " + precision)

// Save and load model
model.save(sc, "myModelPath")
val sameModel = LogisticRegressionModel.load(sc, "myModelPath")

}