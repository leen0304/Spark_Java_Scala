package GCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * User:leen
 * Date:2017/12/15 0015
 * Time:9:38
 */
public class SecondarySortByKey implements Serializable{
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySortByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("A", 10),
                new Tuple2<String, Integer>("D", 20),
                new Tuple2<String, Integer>("D", 6),
                new Tuple2<String, Integer>("B", 6),
                new Tuple2<String, Integer>("C", 12),
                new Tuple2<String, Integer>("B", 2),
                new Tuple2<String, Integer>("A", 3)
        );

        JavaRDD<Tuple2<String,Integer>> rdd1 = sc.parallelize(list);
        JavaPairRDD<String,Integer> pairRdd = rdd1.mapToPair(x -> new Tuple2<String, Integer>(x._1() + " " + x._2(), 1));

        //自定义比较器
        Comparator<String> comparator = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                String[] oo1 = o1.split(" ");
                String[] oo2 = o2.split(" ");
                if (oo1[0].equals(oo2[0])) {
                    return -Integer.valueOf(oo1[1]).compareTo(Integer.valueOf(oo2[1]));
                } else {
                    return oo1[0].compareTo(oo2[0]);
                }
            }
        };

        JavaPairRDD<String,Integer> res = pairRdd.sortByKey(new SecondaryComparator());
        res.foreach(x -> System.out.println(x._1()));
    }

}
