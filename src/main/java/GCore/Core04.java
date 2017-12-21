package GCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * User:leen
 * Date:2017/12/15 0015
 * Time:14:09
 */
public class Core04 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String,Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("A", 90),
                new Tuple2<String, Integer>("C", 60),
                new Tuple2<String, Integer>("C", 50)));
        JavaPairRDD<String,Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("C", 66)));

        JavaPairRDD<String,Tuple2<Integer,Optional<Integer>>> res =  rdd1.leftOuterJoin(rdd2);




        //JavaRDD<Integer> res = rdd.sample(true, 2);
        res.foreach(x -> System.out.print(x + " "));

    }
}
