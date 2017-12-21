import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:chenhaolin 
 * Date:2017/10/14 0014
 * Time:8:53
 */
object Test1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf);
    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt")
    val words = lines.map(line => line.split(" "))
    words.foreach(w =>{
      for (ww <- w)
        println(ww)
    } )
  }
}
