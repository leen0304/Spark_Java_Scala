import java.io.File

/**
 * User:chenhaolin 
 * Date:2017/10/17 0017
 * Time:9:38
 */
object  Utils1 {
  def main(args: Array[String]) {
    deleteDir(new File("C:\\Users\\chenhaolin\\Desktop\\spark1"))
  }

  /**
   * 递归删除文件夹下的目录与文件
   * @param dir
   */
  def deleteDir(dir: File): Unit = {
    if (dir.isDirectory) {
      val list = dir.list()
      for (file <- list) {
        deleteDir(new File(dir, file))
      }
    }
    //删除文件/目录
    dir.delete()
    println("delete :" + dir)
  }
}
