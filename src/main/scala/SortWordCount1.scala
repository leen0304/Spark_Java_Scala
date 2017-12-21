import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:11:03
 */
object SortWordCount1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SortWordCount1").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    val sortWordCount = wordCounts.sortBy(_._2, false)
    sortWordCount.foreach(res => println(res._1 + " : " + res._2))
    sc.stop()
  }

}
