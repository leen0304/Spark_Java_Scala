import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:chenhaolin 
 * Date:2017/10/14 0014
 * Time:8:05
 */
object Persist1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("persist").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt")
    val words = lines.flatMap(line => line.split(" "))

    val s = System.currentTimeMillis()
    val count = words.count()
    val e = System.currentTimeMillis()
    println(e - s +"ms :"+ count)

    val s1 = System.currentTimeMillis()
    val count1 = words.count()
    val e1 = System.currentTimeMillis()
    println(e1 - s1 +"ms :"+ count1)

  }

}
