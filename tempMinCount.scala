import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object tempMinCount extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "CustmerOrdercnt")
  val input = sc.textFile("C:/Users/hp/Downloads/tempdata.csv")
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(3).toInt))
  val minTemp = mappedInput.reduceByKey((x,y) => if(x < y) x else y)
  val result = minTemp.collect()
  result.foreach(println)  
  
}



