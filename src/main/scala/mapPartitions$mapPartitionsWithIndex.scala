import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
 * User:leen
 * Date:2017/11/28 0028
 * Time:16:59
 */
object mapPartitions$mapPartitionsWithIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mapPartitions")
    val sc = new SparkContext(conf)
    //val array = Array(1,2,3,4,5)
    //val rdd = sc.parallelize(1 to 5,3)
    val rdd = sc.makeRDD(1 to 5, 3)

    /** 操作1：同map */
    val res = rdd.mapPartitions(x => {
      val list = new ListBuffer[Integer]()
      while (x.hasNext) {
        list += x.next() + 3
      }
      list.iterator
    });
    res.foreach(x => print(x + "\t")) // 4  5  6  7  8

    /** 操作2：mapPartitions */
    val res1 = rdd.mapPartitions(ite => {
      val list = List[String]()
      var i = 0
      while (ite.hasNext) {
        i += ite.next()
      }
      (i :: list).iterator
    })
    res1.foreach(x => print(x + "\t")) // 1  5  9

    /** 操作3：mapPartitionsWithIndex */
    val res2 = rdd.mapPartitionsWithIndex((index, ite) => {
      val list = List[String]()
      var i = 0
      while (ite.hasNext) {
        i += ite.next()
      }
      (index + "|" + i :: list).iterator
    })
    res2.foreach(x => print(x + "\t")) // 0|1  1|5  2|9
  }
}
