import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
 * User:leen
 * Date:2017/10/17 0017
 * Time:8:30
 */
object BroadcastVariable1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val factor = List[Int](1,2,3);
    val factorBroadcast = sc.broadcast(factor)

    val nums = Array(1,2,3,4,5,6,7,8,9)
    val numsRdd = sc.parallelize(nums,3)
    val list = new ListBuffer[List[Int]]()
    val resRdd = numsRdd.mapPartitions(ite =>{
      while (ite.hasNext){
        list+=ite.next()::(factorBroadcast.value)
      }
      list.iterator
    })

    resRdd.foreach(res => println(res))

    sc.stop()
  }
}
