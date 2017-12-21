import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:chenhaolin 
 * Date:2017/10/10 0010
 * Time:21:26
 */
object Transformation1 {

  def main(args: Array[String]) {
    groupByKey
    //map()
    //filter()
    //flatMap()
    //groupByKey
    //reduceByKey
    //sortBy
    //sortByKey()
    //joinAndCogroup()
  }

  /**
   * map 算子
   */
  def map() {
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)

    //创建list集合
    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //平行化创建RDD
    val numRdd = sc.parallelize(numList)
    //map 操作
    val resRdd = numRdd.map(num => num * 2)
    //遍历
    resRdd.foreach(res => println(res))

  }


  /**
   * filter 算子
   */

  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)

    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numRdd = sc.parallelize(numList)
    val resRdd = numRdd.filter(num => num % 2 == 0)
    resRdd.foreach(res => println(res))
  }

  /**
   * flatMap  算子
   */

  def flatMap(): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt",2)
    val words = lines.flatMap(line => line.split(" "))
    words.foreachPartition(it => {it.foreach(itt => println(itt))})
    //words.foreach(word => println(word))
  }


  /**
   * groupByKey  算子
   */

  def groupByKey(): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    //创建集合
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class1", 30), Tuple2("class2", 67), Tuple2("class2", 66))
    //平行化创建RDD
    val scoreRdd = sc.parallelize(scoreList)
    //groupByKey 操作
    val ress = scoreRdd.groupByKey()
    ress.foreach(println)
    //输出
    ress.foreach(res => {
      println(res._1)
      val scores = res._2
      scores.foreach(score => println(score))
      println("------------------")
    })
  }

  /**
   * reduceByKey  算子
   */
  def reduceByKey(): Unit = {
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class1", 30), Tuple2("class2", 67), Tuple2("class2", 66))
    val scoreRdd = sc.parallelize(scoreList)
    val resRdd = scoreRdd.reduceByKey(_ + _)
    resRdd.foreach(res => println(res._1 + ":" + res._2))
  }

  /**
   * sortBy  算子
   */
  def sortBy(): Unit ={
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class1", 30), Tuple2("class2", 67), Tuple2("class2", 66))
    val scoreRdd = sc.parallelize(scoreList)
    val resRdd = scoreRdd.sortBy(score => score._2,false)
    resRdd.foreach(res => println(res._1 + ":" + res._2))
  }

  /**
   * sortByKey 算子
   */
  def sortByKey(): Unit ={
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class3", 30), Tuple2("class1", 67), Tuple2("class2", 66))
    val scoreRdd = sc.parallelize(scoreList).map(score => (score._2,score._1))
    val resRdd = scoreRdd.sortByKey(false)
    resRdd.foreach(res => println(res._2 + ":" + res._1))
  }

  /**
   *  join  Cogroup() 算子
   */

  def joinAndCogroup(): Unit ={
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList1 = sc.parallelize(Array(Tuple2("class1", "li"), Tuple2("class3", "zahng"), Tuple2("class1", "wang"), Tuple2("class2", "liu")))
    val scoreList2 = sc.parallelize(Array(Tuple2("class1", "guo"), Tuple2("class4", "chen"), Tuple2("class1", "cai"), Tuple2("class2", "yu")))
    val res = scoreList1.cogroup(scoreList2)
    val res1 = scoreList1.join(scoreList2)
    val res2 = scoreList1.leftOuterJoin(scoreList2)
    res.foreach(x => {
      print(x._1 + ":")
      val a = x._2._1.iterator
      for (aa <- a){print(aa + ",")}
      val b = x._2._2.iterator
      for (bb <- b){print(bb + ",")}
      println("")
    })
    println("==================")
    res1.foreach(x => println(x))
    println("==================")
    res2.foreach(x => println(x))

    }
}
