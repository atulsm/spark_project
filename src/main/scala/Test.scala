
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._;

object Test {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val b = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val c = b.zip(a)
    val d = c.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
    println(d.collect.foreach(println))

  }

}

