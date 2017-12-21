import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:19:47
 * 分组之后取出每组的TopN
 */
object GroupTopN1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GroupTopN1").setMaster("local")
    val sc = new SparkContext(conf)

    //val list = Array(1,2,3).toList.sortWith(_ > _).foreach( i => println(i))

    val arry = Array(Tuple2(("c1","u2"),("xp",5)),Tuple2(("c1","u2"),("xz",4)),Tuple2(("c1","u2"),("xw",6)))
    val rdd = sc.parallelize(arry)
    val group = rdd.groupByKey()
    val groupSort = group.map(css =>{
      val c = css._1
      val ss = css._2
      val sortScore = ss.toList.sortBy(_._2)
      (c, sortScore)

    })

    /**
    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\group.txt")
    val classScores = lines.map(line => (line.split(" ")(0), line.split(" ")(1).toInt))
    val group = classScores.groupByKey()
    val groupSort = group.map(css => {
      val c = css._1
      val ss = css._2
      //取出 TopN
      val sortScore = ss.toList.sortWith(_ < _).take(3)
      (c, sortScore)
    })

    groupSort.foreach(v => {
      print(v._1 + " : ")
      v._2.foreach(ss => print("\t" + ss))
      println()
    })
*/
    groupSort.foreach(x => println(x))
    sc.stop()
  }
}
