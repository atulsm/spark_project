

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark._
import org.apache.spark.graphx.lib._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object SparkShortestPath extends App {
  val rootLogger = Logger.getLogger("org")
  rootLogger.setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val articles = sc.textFile("C:/BigData/Apache Spark/Spark-in-action/first-edition-master/ch09/articles.tsv", 6).
    filter(line => line.trim() != "" && !line.startsWith("#")).zipWithIndex()

  articles.filter(x => x._1 == "Rainbow" || x._1 == "14th_century")
    .collect()
    .foreach(println)

  articles.count()

  val links = sc.textFile("C:/BigData/Apache Spark/Spark-in-action/first-edition-master/ch09/links.tsv", 6)
    .filter(line => line.trim() != "" && !line.startsWith("#"))

  val linkIndexes = links.map(x => { val spl = x.split("\t"); (spl(0), spl(1)) }).
    join(articles).map(x => x._2).join(articles).map(x => x._2)

  val wikigraph = Graph.fromEdgeTuples(linkIndexes, 0)
  wikigraph.vertices.count()
  //

  //  //Long = 4604
  linkIndexes.map(x => x._1).union(linkIndexes.map(x => x._2)).distinct().count()
  //Long = 4592
  //finding the articles that have no links
  val distinctLinkIndexes = linkIndexes.map(x => x._1)
    .union(linkIndexes.map(x => x._2))
    .distinct().map(x => (x, x))
  articles.map(x => (x._2, x._1)).leftOuterJoin(distinctLinkIndexes).filter(x => x._2._2.isEmpty).collect()

  //
  //  //shortest path
  //
  val shortest = ShortestPaths.run(wikigraph, Seq(10))
  shortest.vertices.filter(x => x._1 == 3425).collect.foreach(println)

  // page rank

  val ranked = wikigraph.pageRank(0.001)
  val ordering = new Ordering[Tuple2[VertexId, Double]] {
    def compare(x: Tuple2[VertexId, Double], y: Tuple2[VertexId, Double]): Int = x._2.compareTo(y._2)
  }
  val top10 = ranked.vertices.top(10)(ordering)
  sc.parallelize(top10).join(articles.map(_.swap))
    .collect.sortWith((x, y) => x._2._1 > y._2._1).foreach(println)

  // connected components 

  //  val wikiCC = wikigraph.connectedComponents()
  //  wikiCC.vertices.map(x => (x._2, x._2)).distinct().join(articles.map(_.swap)).collect.foreach(println)
  //  wikiCC.vertices.map(x => (x._2, x._2)).countByKey().foreach(println)

  // strongly connected 
  //  val wikiSCC = wikigraph.stronglyConnectedComponents(100)
  //  wikiSCC.vertices.map(x => x._2).distinct.count
  //  // 519
  //  wikiSCC.vertices.map(x => (x._2, x._1)).countByKey().filter(_._2 > 1).
  //    toList.sortWith((x, y) => x._2 > y._2).foreach(println)
  //
  //  wikiSCC.vertices.filter(x => x._2 == 2488).join(articles.map(x => (x._2, x._1))).collect.foreach(println)
  //
  //  wikiSCC.vertices.filter(x => x._2 == 1831).join(articles.map(x => (x._2, x._1))).collect.foreach(println)
  //
  //  wikiSCC.vertices.filter(x => x._2 == 892).join(articles.map(x => (x._2, x._1))).collect.foreach(println)

}