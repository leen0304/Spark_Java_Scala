import org.apache.spark.{SparkConf, SparkContext}

/**
 * User:chenhaolin 
 * Date:2017/10/9 0009
 * Time:17:19
 */
object WordCount1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt", 2)

    val words = lines.flatMap(line => line.split(" "))

    val parts = words.map( word => (word,1))

    val res = parts.reduceByKey(_ + _)

    res.foreach( pair => println(pair._1 + ":" + pair._2 ))
  }
}
