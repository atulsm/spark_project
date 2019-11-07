

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.classification.RandomForestClassifier
object SparkRandomForest extends App {
  val rootLogger = Logger.getLogger("org")
  rootLogger.setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  import spark.implicits._

  val penschema = StructType(Array(
    StructField("pix1", DoubleType, true),
    StructField("pix2", DoubleType, true),
    StructField("pix3", DoubleType, true),
    StructField("pix4", DoubleType, true),
    StructField("pix5", DoubleType, true),
    StructField("pix6", DoubleType, true),
    StructField("pix7", DoubleType, true),
    StructField("pix8", DoubleType, true),
    StructField("pix9", DoubleType, true),
    StructField("pix10", DoubleType, true),
    StructField("pix11", DoubleType, true),
    StructField("pix12", DoubleType, true),
    StructField("pix13", DoubleType, true),
    StructField("pix14", DoubleType, true),
    StructField("pix15", DoubleType, true),
    StructField("pix16", DoubleType, true),
    StructField("label", DoubleType, true)))

  val pen_raw = sc.textFile("C:/BigData/Apache Spark/Spark-in-action/first-edition-master/ch08/penbased.dat", 4)
    .map(x => x.split(", ")).
    map(row => row.map(x => x.toDouble))

  import org.apache.spark.sql.Row
  val dfpen = spark.createDataFrame(pen_raw.map(Row.fromSeq(_)), penschema)
  import org.apache.spark.ml.feature.VectorAssembler
  val va = new VectorAssembler().setOutputCol("features")
  va.setInputCols(dfpen.columns.diff(Array("label")))
  val penlpoints = va.transform(dfpen).select("features", "label")

  val dtsi = new StringIndexer().setInputCol("label").setOutputCol("label-ind")
  val dtsm: StringIndexerModel = dtsi.fit(penlpoints)
  val pendtlpoints = dtsm.transform(penlpoints).drop("label").withColumnRenamed("label-ind", "label")

  val pendtsets = pendtlpoints.randomSplit(Array(0.8, 0.2))
  val pendttrain = pendtsets(0).cache()
  val pendtvalid = pendtsets(1).cache()

  val rf = new RandomForestClassifier()
  rf.setMaxDepth(22)
  //rf.setNumTrees(3)
  val rfmodel = rf.fit(pendttrain)
  println(rfmodel)
  val rfpredicts = rfmodel.transform(pendtvalid)
  val rfresrdd = rfpredicts.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

  rfresrdd.collect.foreach(println)
  val rfmm = new MulticlassMetrics(rfresrdd)
  println(rfmm.precision)
  //0.9894640403114979
  println(rfmm.confusionMatrix)

}
