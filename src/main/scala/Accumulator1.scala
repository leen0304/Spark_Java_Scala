import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkContext, SparkConf}

/**
 * User: leen
 * Date: 2017/10/18 0018
 * Time: 10:28
 */
object Accumulator1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Accumulator1").setMaster("local")
    val sc = new SparkContext(conf)

    val myAcc = new MyAccumulator
    sc.register(myAcc,"myAcc")


    //val acc = sc.longAccumulator("avg")
    val nums = Array("1","2","3","4","5","6","7","8")
    val numsRdd = sc.parallelize(nums)

    //numsRdd.map(x => acc.add(x))


    numsRdd.foreach(num => myAcc.add(num))
    println(myAcc.value)
    sc.stop()
  }


}

class  MyAccumulator extends AccumulatorV2[String,String]{

  private var res = ""
  override def isZero: Boolean = {res == ""}

  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case o : MyAccumulator => res += o.res
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def copy(): MyAccumulator = {
    val newMyAcc = new MyAccumulator
    newMyAcc.res = this.res
    newMyAcc
  }

  override def value: String = res

  override def add(v: String): Unit = res += v +"-"

  override def reset(): Unit = res = ""
}
