import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object wordcount extends App {
  
    Logger.getLogger("org").setLevel(Level.ERROR)
  
    val sc = new SparkContext("local[*]", "wordcount")
  
    val input = sc.textFile("C:/Users/hp/Downloads/search_data.txt")
    
    val words = input.flatMap(x => x.split(" "))
    
    val wordslower = words.map(_.toLowerCase())
    
    val map_word = wordslower.map(x => (x,1))
    
    val sum_word = map_word.reduceByKey((x,y) => x+y)
    
    val revtuple = sum_word.map(x => (x._2, x._1))
    
    val sortedResult = revtuple.sortByKey(false).map(x => (x._2, x._1))
    
    sortedResult.collect.foreach(println)
    
    scala.io.StdIn.readLine()
    
}
  
