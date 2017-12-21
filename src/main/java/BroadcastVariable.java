import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * User:leen
 * Date:2017/10/15 0015
 * Time:10:01
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("broadcastVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        /**
         * 做成广播变量 【只读】
         * 通过 sc.broadcast() 方法将变量做成广播变量的形式
         * 返回类型为 BroadCast<T>
         * 获取的方式为 getValue() 方法
         */
        int factor = 3;
        final Broadcast<Integer> factorBroadCast = sc.broadcast(factor);

        List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        JavaRDD<Integer> numRdd = sc.parallelize(numList);

        JavaRDD<Integer> res = numRdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                int fac = factorBroadCast.getValue();
                return v1 * fac;
            }
        });

        res.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }
}
