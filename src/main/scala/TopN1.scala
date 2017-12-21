import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:17:34
 */
object TopN1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TopN1").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\score.txt")
    val sortedScores = lines.sortBy(line => line.toInt, false)
    val top3 = sortedScores.take(3)
    for (t <- top3) {
      println(t)
    }
    sc.stop()
  }
}
