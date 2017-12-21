import org.apache.spark.{SparkContext, SparkConf}

/**
 * User:leen
 * Date:2017/11/28 0028
 * Time:13:54
 */
object Coalesce1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("coalesce1").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9),8)
    val num = rdd.partitions.length //num=8

    val rdd1 = rdd.coalesce(10,false);
    val num1 = rdd1.partitions.length //num1=8

    val rdd2 = rdd.coalesce(10,true);
    val num2 = rdd2.partitions.length //num2=10

    val rdd3 = rdd.coalesce(2,false);
    val num3 = rdd3.partitions.length //num3=2


  }
}
