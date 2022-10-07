import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
object movieRatingCount extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)
     
    val sc = new SparkContext("local[*]", "movieRatingcnt")
    val input = sc.textFile("C:/Users/hp/Downloads/moviedata.data")
    val mappedInput = input.map(x => x.split("\t")(2))
    val result = mappedInput.countByValue()
     result.foreach(println)  
//    val ratings = mappedInput.map(x => (x, 1))
//    val reduceRatings = ratings.reduceByKey((x,y) => x+y)
//    val sortedRatings = reduceRatings.sortByKey(false)
//    val result = sortedRatings.collect
//   result.foreach(println)   
}