/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:14:16
 *
 * 比较器：
 * 1. 继承 Ordered[T] 和Serializeable 方法
 * 2. 重写 compare 方法
 * 3. 需要传入参数
 */
class SecondarySortKey1(val first: Int, val second: Int) extends Ordered[SecondarySortKey1] with Serializable {
  override def compare(that: SecondarySortKey1): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first    //第一个数 排序：升序
    } else {
      that.second - this.second   //第二个数 排序：降序
    }
  }
}
