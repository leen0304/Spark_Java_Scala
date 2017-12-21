package GCore


import java.io.File

import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * User:leen
 * Date:2017/12/9 0009
 * Time:10:51
 */
object Core03 {

  def main(args: Array[String]) {
    cogroup()
  }

  def cogroup(): Unit ={
    val conf = new SparkConf().setAppName("cogroup").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("A","3"),("A","2"),("C","3"),("A","a"),("A","4"),("D","d"),("A","3"),("A","2"),("C","3"),("A","a"),("A","4"),("D","d")),1)
    val rdd2 = sc.parallelize(Array(("A","a"),("C","c"),("D","d")),2)


   val res =  rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    //val res = rdd1.cogroup(rdd2)
    res.foreach(x=>{println(x)})
  }

  def lookup(): Unit ={
    val conf = new SparkConf().setAppName("cogroup").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("A","1"),("B","2"),("C","3"),("A","a"),("C","c"),("D","d")),2)
//rdd1.saveAsTextFile("C:\\Users\\chenhaolin\\Desktop\\spark1")
    //rdd1.saveAsHadoopFile("C:\\Users\\chenhaolin\\Desktop\\spark1")
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapred.output.compress", "true")
    hadoopConf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
rdd1.saveAsNewAPIHadoopFile("C:\\Users\\chenhaolin\\Desktop\\spark1",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]],hadoopConf)
    val res = rdd1.lookup("A")
    print(res)
  }



  def aggregateOp(): Unit ={
    val conf = new SparkConf().setAppName("aggregateOp").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List("aa","bb","cc")
    val rdd = sc.parallelize(list)

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
   * 求学生成绩平均值
   */
  def avgScore(): Unit = {
    val conf = new SparkConf().setAppName("avgScore").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //2.1 构建学生信息list集合
    val scoreDetail = List(
      ScoreDetail("A", "Math", 98),
      ScoreDetail("A", "English", 88),
      ScoreDetail("B", "Math", 75),
      ScoreDetail("B", "English", 78),
      ScoreDetail("C", "Math", 90),
      ScoreDetail("C", "English", 80),
      ScoreDetail("D", "Math", 91),
      ScoreDetail("D", "English", 80)
    )
    //2.2 创建学生信息Tuple2(学生名称，学生信息)
    val studentDetail = for {x <- scoreDetail} yield (x.studentName, x)
    //2.3 平行化学生信息，并创建Hash分区，缓存
    val studentDetailRdd = sc.parallelize(studentDetail).partitionBy(new HashPartitioner(3)).cache()

    val avgscoreRdd = studentDetailRdd.combineByKey(
      // createCombiner：组合器函数，输入参数为RDD[K,V]中的V（即ScoreDetail对象），输出为tuple2（学生成绩，1）
      (x: ScoreDetail) => (x.score, 1),
      // mergeValue：合并值函数，输入参数为(C,V)即（tuple2（学生成绩，1），ScoreDetail对象），输出为tuple2（学生成绩，2）
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1),
      // mergeCombiners：合并组合器函数，对多个节点上的数据合并，输入参数为(C,C)，输出为C
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map(x => (x._1, x._2._1 / x._2._2)) //对于输出(学生姓名，(学生成绩和，学生成绩次数))，求学生成绩平均值

    avgscoreRdd.foreach(println)
  }
}

case class ScoreDetail(studentName: String, subject: String, score: Float)

