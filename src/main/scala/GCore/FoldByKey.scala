package GCore

import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkEnv, Partitioner, SparkContext, SparkConf}

/**
 * User:leen
 * Date:2017/12/12 0012
 * Time:15:50
 */
object FoldByKey {
  def main(args: Array[String]) {
    sortByKey
  }

  def foldByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("FoldByKey")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(("a",0),("a",2),("b",1),("b",2)),4)


    val res =rdd.foldByKey(0)(_+_)
    res.foreach(println)
  }

  def sortBy(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sortBy")
    val sc = new SparkContext(conf)
    //val rdd = sc.makeRDD(List(("a",0),("d",5),("d",2),("c",1),("b",2),("b",0)))
    val rdd = sc.makeRDD(List(5,1,9,12),2)

    val res =rdd.sortBy(x=>x,false,1)
    res.foreach(println)
  }

  def sortByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sortBy")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List((11,0),(10,5),(4,2),(6,1),(20,2),(8,0)))

    val res =rdd.sortByKey(true,1)
    res.foreach(x => print(x + " "))
  }

  def fold(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("FoldByKey")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List("a","a","b","b"),4)
    val res = rdd.fold("A")(_+_)
    println(res)
  }

  def aggregateOp(): Unit ={
    val conf = new SparkConf().setAppName("aggregateOp").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List("aa","bb","cc")
    val rdd = sc.parallelize(list,2)

    def seqOp(a:String,b:String): String ={
      a+"-"+b
    }
    def combOp(a:String,b:String): String ={
      a+":"+b
    }
    val res = rdd.aggregate("oo")(seqOp,combOp)

    println(res)
  }
  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which may be added to the result an arbitrary number of times,
   * and must not change the result (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   *
   */


}
