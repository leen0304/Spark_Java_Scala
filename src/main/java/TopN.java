import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.List;

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:15:03
 */
public class TopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\score.txt");
        JavaRDD<Integer> scores = lines.map(new Function<String, Integer>() {
            public Integer call(String v1) throws Exception {
                return Integer.valueOf(v1);
            }
        });

        /**
         * 此处使用 sortBy() 算子
         * 必须传入三个参数，没有相关的其他构造函数
         */
        JavaRDD<Integer> resRdd = scores.sortBy(new Function<Integer, Object>() {
            public Object call(Integer v1) throws Exception {
                return v1;
            }
        }, true, 1);

        List<Integer> res  = resRdd.take(3);

        for(Integer i : res){
            System.out.println(i);
        }
        sc.close();
    }
}
