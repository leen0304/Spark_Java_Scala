import java.io.File;

/**
 * User:chenhaolin
 * Date:2017/10/17 0017
 * Time:9:51
 */
public class Utils {

    /**
     * 递归删除目录中的子目录下
     * @param dir
     */
    public static void deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] files = dir.list();
            for (int i = 0; i < files.length; i++) {
                deleteDir(new File(dir, files[i]));
            }
        }
        dir.delete();
        System.out.println("delete :" + dir);
    }
}
