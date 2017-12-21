import java.io.File;

/**
 * User: leen
 * Date: 2017/10/17 0017
 * Time: 19:19
 */
public class Test {
    public static void main(String[] args) {
        File file = new File("C:\\Users\\chenhaolin\\Desktop\\11707037郭璐明材料");
        if(file.isDirectory()){
            String[] files = file.list();
            for (int i = 0; i < files.length ; i++) {
                System.out.println(files[i]);
            }
        }
    }


}
