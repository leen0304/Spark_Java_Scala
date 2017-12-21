import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Iterator;

/**
 * User:leen
 * Date:2017/10/13 0013
 * Time:9:09
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("persist").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt");
        /**
         * cache() 和 persist 的使用是有规则的
         * 必须在 transformation 或者 textFile 之后直接连续调用cache 或者 persist 才有效
         * 如果创建一个Rdd之后，在单独另起一行调用Cache 或者 persist 是不生效的
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }).persist(StorageLevel.MEMORY_ONLY_2());

        /**
         * 1. MEMORY_ONLY() 以非序列化的方式持久化到JVM的内存中，如果内存无法完全存储RDD的partition ,那么没有持久化的partition会在下次调用的时候，重新计算
         * 2. MEMORY_AND_DISK() 同上，但是无法存储在内存中的partition 会被存储在磁盘中；
         * 3. MEMORY_ONLY_SER() 会使用Java序列化的方式，先将Java对象序列化之后进行持久化  ，消耗CPU
         * 4. MEMORY_ONLY_2()  将持久化的数据备用一份，保存在其他节点
         */
        long start = System.currentTimeMillis();
        long l = words.count();
        long end = System.currentTimeMillis();
        System.out.println((end - start )  +"ms: "+l);

        long start1 = System.currentTimeMillis();
        long l1 = words.count();
        long end1 = System.currentTimeMillis();
        System.out.println((end1 - start1 ) +"ms: "+l1);
    }
}













