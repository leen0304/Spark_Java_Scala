import java.io.File

import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.io.compress.{DefaultCodec, Lz4Codec, GzipCodec, BZip2Codec}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * User:chenhaolin 
 * Date:2017/10/14 0014
 * Time:11:41
 */
object Action1 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Action").setMaster("local")
    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration;

    hadoopConf.set(TableOutputFormat.OUTPUT_TABLE,"")

    val nums = Array(1,2,3,4,5,6,7,8,9)
    //val numsRdd = sc.parallelize(nums)
    val numsRdd = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\ziyuzile\\black03.txt")

    val file = new File("C:\\Users\\chenhaolin\\Desktop\\spark1");
    Utils1.deleteDir(file)
    val saveAsTextFile = numsRdd.saveAsTextFile(file.toString,classOf[GzipCodec])
  }
}
