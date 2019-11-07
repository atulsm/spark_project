

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
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }

import org.apache.spark.ml.param.ParamMap
object SparkLogisticMultiRegression extends App {
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

  val pensets = penlpoints.randomSplit(Array(0.8, 0.2))
  val pentrain = pensets(0).cache()
  val penvalid = pensets(1).cache()

  val penlr = new LogisticRegression().setRegParam(0.001)
  import org.apache.spark.ml.classification.OneVsRest
  val ovrest = new OneVsRest()
  ovrest.setClassifier(penlr)

  val ovrestmodel = ovrest.fit(pentrain)

  val penresult = ovrestmodel.transform(penvalid)
  val penPreds = penresult.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

  //penPreds.collect.foreach(println)
  import org.apache.spark.mllib.evaluation.MulticlassMetrics

  val penmm = new MulticlassMetrics(penPreds)
  //println(penmm.precision)
  //0.9018214127054642
  println(penmm.precision(3))
  //0.9026548672566371
  println(penmm.recall(3))
  //0.9855072463768116
  println(penmm.fMeasure(3))
  //0.9422632794457274
  println(penmm.confusionMatrix)

  //val lrmodel = lr.fit(adulttrain)

  def computePRCurve(train: DataFrame, valid: DataFrame, lrmodel: LogisticRegressionModel) =
    {
      import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
      for (threshold <- 0 to 10) {
        var thr = threshold / 10.0
        if (threshold == 10)
          thr -= 0.001
        lrmodel.setThreshold(thr)
        val validpredicts = lrmodel.transform(valid)
        val validPredRdd = validpredicts.rdd.map(row => (row.getDouble(4), row.getDouble(1)))
        val bcm = new BinaryClassificationMetrics(validPredRdd)
        val pr = bcm.pr.collect()(1)
        println("%.1f: R=%f, P=%f".format(thr, pr._1, pr._2))
      }
    }

  def computeROCCurve(train: DataFrame, valid: DataFrame, lrmodel: LogisticRegressionModel) =
    {
      import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
      for (threshold <- 0 to 10) {
        var thr = threshold / 10.0
        if (threshold == 10)
          thr -= 0.001
        lrmodel.setThreshold(thr)
        val validpredicts = lrmodel.transform(valid)
        val validPredRdd = validpredicts.rdd.map(row => (row.getDouble(4), row.getDouble(1)))
        val bcm = new BinaryClassificationMetrics(validPredRdd)
        val pr = bcm.roc.collect()(1)
        println("%.1f: FPR=%f, TPR=%f".format(thr, pr._1, pr._2))
      }
    }

  def oneHotEncodeColumns(df: DataFrame, cols: Array[String]): DataFrame = {
    import org.apache.spark.ml.feature.OneHotEncoder
    var newdf = df
    for (c <- cols) {
      val onehotenc = new OneHotEncoder().setInputCol(c)
      onehotenc.setOutputCol(c + "-onehot").setDropLast(false)
      newdf = onehotenc.transform(newdf).drop(c)
      newdf = newdf.withColumnRenamed(c + "-onehot", c)
    }
    newdf
  }

  def indexStringColumns(df: DataFrame, cols: Array[String]): DataFrame = {
    import org.apache.spark.ml.feature.StringIndexer
    import org.apache.spark.ml.feature.StringIndexerModel
    //variable newdf will be updated several times
    var newdf = df
    for (c <- cols) {
      val si = new StringIndexer().setInputCol(c).setOutputCol(c + "-num")
      val sm: StringIndexerModel = si.fit(newdf)
      newdf = sm.transform(newdf).drop(c)
      newdf = newdf.withColumnRenamed(c + "-num", c)
    }
    newdf
  }

}
