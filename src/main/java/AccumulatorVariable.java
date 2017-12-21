import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * User: leen
 * Date: 2017/10/18 0018
 * Time: 10:13
 */
public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建共享变量
        //调用sc 的 accumulator() 方法
        final Accumulator<Integer> acc = sc.accumulator(0);
        //创建集合
        List<Integer> list = Arrays.asList(1, 2, 3, 1, 2, 5, 6);
        //平行化RDD
        JavaRDD<Integer> numRdd = sc.parallelize(list);

        //之后，在函数内部，就可以对accumulator变量，调用add() 方法进行累加。
        numRdd.foreach(new VoidFunction<Integer>() {
            public void call(Integer num) throws Exception {
                acc.add(num);
            }
        });
        System.out.println(acc);
        sc.close();

    }
}
