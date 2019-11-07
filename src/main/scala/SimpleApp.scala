/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SimpleApp {
  

 
  def myfilter(line: String): Boolean = {
    
    line.contains("a")
   
    }
  
  def main(args: Array[String]) {
   // val logFile = "file:////usr/hdp/2.4.0.0-169/spark/README.md" // Should be some file on your system set("spark.ui.port", "44040" ).
    val rootLogger = Logger.getLogger("org")
    rootLogger.setLevel(Level.ERROR)
    //val logFile= "hdfs://ip-172-31-53-48.ec2.internal:8020/user/gl_faculty4246/input"
    
     //val logFile= "C:\\BigData\\logdata.txt"
     val logFile= "/input"
    
    val conf = new SparkConf().setAppName(" My first spark Application")
    //.setMaster("local[2]");
    val sc1 = new SparkContext(conf)
    val logData = sc1.textFile(logFile,3)
   
    sc1.setCheckpointDir("/checkpoint");
    logData.checkpoint
    
    var partitionRDD = logData.repartition(2)
    //partitionRDD.cache
    
    partitionRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    
    val numAs = partitionRDD.filter(myfilter).map(line => ("LineA",1)).reduceByKey(_+_)
    .saveAsTextFile("/spark_linecount_A")
    val numBs = partitionRDD.filter(myline => myline.contains("b")).map(line => ("LineB",1))
    .reduceByKey(_+_).saveAsTextFile("/spark_linecount_B")
    
    //saveAsTextFile("/user/gl_faculty4246/spark_wc3")
    
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    
       
    Thread.sleep(10000000)
  }
}

