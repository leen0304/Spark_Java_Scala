import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:10:35
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairRDD<String, Integer> wordsCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> countWords = wordsCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> wordCount) throws Exception {
                return new Tuple2<Integer, String>(wordCount._2(), wordCount._1());
            }
        });
        JavaPairRDD<Integer, String> sortCountWords = countWords.sortByKey(false);
        JavaPairRDD<String, Integer> sortWordCounts = sortCountWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> sortCountWord) throws Exception {
                return new Tuple2<String, Integer>(sortCountWord._2(), sortCountWord._1());
            }
        });
        sortWordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> sortWordCounts) throws Exception {
                System.out.println(sortWordCounts._1() + " : " + sortWordCounts._2());
            }
        });
        sc.close();
    }
}
