package GCore

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

/**
 * User:leen
 * Date:2017/12/18 0018
 * Time:13:42
 */
object Core005 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("core05")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5), 2)


    //类似Java的mapPartitionsToPair
    val rdd1 = rdd.mapPartitions(ite => {
      //val list = new ListBuffer[Tuple2[Integer, Integer]](); //scala.collection.mutable.ListBuffer
      var list = List[Tuple2[Int, Int]]()
      while (ite.hasNext) {
        val next = ite.next()
        list = Tuple2(next, next * 2) ::list
      }
      list.iterator
    }, false)

    rdd1.foreach(x => print(x + " "))

    //mapPartitions
    val rdd2 = rdd.mapPartitions(ite => {
      val list = new ListBuffer[Integer](); //scala.collection.mutable.ListBuffer
      while (ite.hasNext) {
        val next = ite.next()
        list += next
      }
      list.iterator
    }, false)

    //rdd2.foreach(x => print(x + " "))

    //mapPartitionsWithIndex
    val rdd3 = rdd.mapPartitionsWithIndex((index, ite) => {
      val map = Map[String,List[Integer]]() //scala.collection.mutable.Map
      val indexStr = "part-" + index
      while (ite.hasNext) {
        if (map.contains(indexStr)) {
          var tmpList = map(indexStr)
          tmpList = ite.next() :: tmpList
          map(indexStr) = tmpList
        } else {
          map(indexStr) = List[Integer](ite.next())
        }
      }
      map.iterator
    }, false)

    //rdd3.foreach(x => print(x + " "))
  }

}
