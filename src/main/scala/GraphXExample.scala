
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.apache.spark._

import org.apache.spark.graphx._
object GraphXExample extends App {
  val conf = new SparkConf().setAppName("Simple Application").set("spark.driver.allowMultipleContexts", "true").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val bigdataMembers: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((5L, ("Anand", "Architect")), (3L, ("Hari", "Manager")),
      (4L, ("Sandeep", "developer")), (6L, ("Mukesh", "SQL expert"))))

  val relationships: RDD[Edge[String]] =

    sc.parallelize(Array(Edge(6, 5, "Team member"), Edge(5, 3, "Reporting"),
      Edge(4, 6, "Peers")))
  val defualtUser = ("David", "Work not yet assigned")

  val subex_graph = Graph(bigdataMembers, relationships, defualtUser)

  println(subex_graph.vertices.filter {
    case (id, (name, designation)) =>
      designation == "Manager"
  }.count)

  println(subex_graph.edges.filter { e => e.srcId > e.dstId }.count)

  val path: RDD[String] = subex_graph.triplets.map(triplet => triplet.srcAttr._1 + " playing role of "+ triplet.srcAttr._2+
     " is the " + triplet.attr + "of " + triplet.dstAttr._1+" playing role "+triplet.dstAttr._2)

  path.collect.foreach(println)
  //Thread.sleep(2222222)
}