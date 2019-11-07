

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
object SparkKmeans extends App {
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
    StructField("pix1", IntegerType, true),
    StructField("pix2", IntegerType, true),
    StructField("pix3", IntegerType, true),
    StructField("pix4", IntegerType, true),
    StructField("pix5", IntegerType, true),
    StructField("pix6", IntegerType, true),
    StructField("pix7", IntegerType, true),
    StructField("pix8", IntegerType, true),
    StructField("pix9", IntegerType, true),
    StructField("pix10", IntegerType, true),
    StructField("pix11", IntegerType, true),
    StructField("pix12", IntegerType, true),
    StructField("pix13", IntegerType, true),
    StructField("pix14", IntegerType, true),
    StructField("pix15", IntegerType, true),
    StructField("pix16", IntegerType, true),
    StructField("label", IntegerType, true)))

  val pen_raw = sc.textFile("C:/BigData/Apache Spark/Spark-in-action/first-edition-master/ch08/penbased.dat", 4)
    .map(x => x.split(", ")).
    map(row => row.map(x => x.toDouble.toInt))

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

  import org.apache.spark.ml.clustering.KMeans
  val kmeans = new KMeans()
  kmeans.setK(10)
  kmeans.setMaxIter(500)
  val kmmodel = kmeans.fit(penlpoints)

  println(kmmodel.computeCost(penlpoints))
  //4.517530920539787E7
  println(math.sqrt(kmmodel.computeCost(penlpoints) / penlpoints.count()))
  //67.5102817068467

  val kmpredicts = kmmodel.transform(penlpoints)

  printContingency(kmpredicts, 0 to 9)

  import org.apache.spark.rdd.RDD
  //df has to contain at least two columns named prediction and label
  def printContingency(df: org.apache.spark.sql.DataFrame, labels: Seq[Int]) {
    val rdd: RDD[(Int, Int)] = df.select('label, 'prediction).rdd.map(row => (row.getInt(0), row.getInt(1))).cache()
    val numl = labels.size
    val tablew = 6 * numl + 10
    var divider = "".padTo(10, '-')
    for (l <- labels)
      divider += "+-----"

    var sum: Long = 0
    print("orig.class")
    for (l <- labels)
      print("|Pred" + l)
    println
    println(divider)
    val labelMap = scala.collection.mutable.Map[Int, (Int, Long)]()
    for (l <- labels) {
      //filtering by predicted labels
      val predCounts = rdd.filter(p => p._2 == l).countByKey().toList
      //get the cluster with most elements
      val topLabelCount = predCounts.sortBy { -_._2 }.apply(0)
      //if there are two (or more) clusters for the same label
      if (labelMap.contains(topLabelCount._1)) {
        //and the other cluster has fewer elements, replace it
        if (labelMap(topLabelCount._1)._2 < topLabelCount._2) {
          sum -= labelMap(l)._2
          labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
          sum += topLabelCount._2
        }
        //else leave the previous cluster in
      } else {
        labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
        sum += topLabelCount._2
      }
      val predictions = predCounts.sortBy { _._1 }.iterator
      var predcount = predictions.next()
      print("%6d".format(l) + "    ")
      for (predl <- labels) {
        if (predcount._1 == predl) {
          print("|%5d".format(predcount._2))
          if (predictions.hasNext)
            predcount = predictions.next()
        } else
          print("|    0")
      }
      println
      println(divider)
    }
    rdd.unpersist()
    println("Purity: " + sum.toDouble / rdd.count())
    println("Predicted->original label map: " + labelMap.mapValues(x => x._1))

    // Purity: 0.6672125181950509
    // Predicted->original label map: Map(8 -> 8, 2 -> 6, 5 -> 0, 4 -> 1, 7 -> 5, 9 -> 9, 3 -> 4, 6 -> 3, 0 -> 2)
  }

}
