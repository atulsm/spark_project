import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.StorageLevels

object WordCountExample {
  
  def splitLine(line:String): Array[String] = {
    
    line.split(" ")
    
  }
  
  
  def main(args: Array[String]) {
   // val logFile = "file:////usr/hdp/2.4.0.0-169/spark/README.md" // Should be some file on your system set("spark.ui.port", "44040" ).
    
    val logFile= "/tmp/emp_Asia.txt"
    
    val conf = new SparkConf().setAppName("WordCount Application").setMaster("local[2]") 
    val sc1 = new SparkContext(conf)
    
    
    sc1.setCheckpointDir("/tmp/checkpoint")
    
    val logData = sc1.textFile(logFile)   
    
    val partRDD = logData.repartition(6)
   
   
    
    //logData.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    var result = partRDD.flatMap(myline => myline.split(" "))
    result.cache
    result.checkpoint()
    
    //result.persist(StorageLevels.MEMORY_AND_DISK_SER)
    
  
    var pairRDD = result.map(myword => (myword,1))
    
    
    
    var finalout = pairRDD.reduceByKey((a,b) => a+b)
    
    //finalout.checkpoint()
    
   // var sortRDD= finalout.sortByKey()
    
   // finalout.collect.foreach(println)
    
    //sortRDD.saveAsTextFile("/spark_out")
    
    //result.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
   var hadoopRdd=  result.filter( data => data.contains("hadoop"))
  
   
   print(hadoopRdd.count())
   
    
    
    //var partRdd = result.repartition(6)
    
    //partRdd.checkpoint
    
    //val pardRdd1  = partRdd.map(word => (word,1))
    
    // I [1,1,1]
    
   // 1+1 = 2 + 1= 3
    
    
    
   //val finalout=  pardRdd1.reduceByKey(_+_)
    
    
    
    
   // val pardRdd1= partRdd.map(word => (word,1)).reduceByKey(_+_)
    
    // hadoop <1,1,1,1> 2+1= 3+1=4
    
    
    //finalout.collect.foreach(println)
    
    
    
   finalout.saveAsTextFile("/tmp/output")
 
   //result.saveAsSequenceFile("/wc_cisco1")
   //result.saveAsObjectFile("/wc_cisco2")
  
   //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    
    Thread.sleep(10000000)
  }
}