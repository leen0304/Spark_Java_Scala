/**
 * User:leen
 * Date:2017/11/28 0028
 * Time:17:34
 */
object ScalaSeq {
  def main(args: Array[String]) {
    val list1 = List(1,2,3,4,5)
    val list2 = 1::1::Nil;
    list2.foreach(x => println(x))
  }

}
