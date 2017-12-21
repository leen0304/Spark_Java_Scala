import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:14:23
 */
object SecondarySort1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SecondarySort1").setMaster("local");
    val sc = new SparkContext(conf)

    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\sort.txt")
    //与自定义Key组合返回Tuple2
    val words = lines.map(line => {
      val ws = line.split(" ")
      (new SecondarySortKey1(ws(0).toInt, ws(1).toInt), line)
    })
    val sortRes = words.sortByKey()
    sortRes.foreach(pair => println(pair._2))

    sc.stop()
  }
}
