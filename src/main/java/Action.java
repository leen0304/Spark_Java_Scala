import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * User:chenhaolin
 * Date:2017/10/14 0014
 * Time:11:41
 */
public class Action {
    public static void main(String[] args) {
        reduce();
    }

    /**
     * Reduce 算子操作
     */
    private static void reduce() {

        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> num = Arrays.asList(1, 2, 4, 5, 7, 8);
        JavaRDD<Integer> numRdd = sc.parallelize(num);
        //collect
        List<Integer> collectList = numRdd.collect();
        //take
        List<Integer> takeList = numRdd.take(3);
        //count
        Long count = numRdd.count();
        //saveAsTextFile
        File file = new File("C:\\Users\\chenhaolin\\Desktop\\spark1");
        Utils.deleteDir(file);
        numRdd.saveAsTextFile(String.valueOf(file));
        //reduce
        Integer res = numRdd.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(res);
        System.out.println("----------------");
        for (Integer i : collectList) {
            System.out.println(i);
        }
        System.out.println("----------------");
        for (Integer i : takeList) {
            System.out.println(i);
        }
        System.out.println("----------------");
        System.out.println(count);
        System.out.println("----------------");
    }
}














