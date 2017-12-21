package GCore;

import org.apache.parquet.hadoop.codec.SnappyCodec;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * User:leen
 * Date:2017/12/7 0007
 * Time:10:43
 */
public class Core01 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Core01").setMaster("local");
        //conf.set("spark.rdd.compress", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Accumulator<Integer> counter = sc.accumulator(0);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaRDD<String> filterWords = words.filter(word -> word.startsWith("s"));

        filterWords.foreach(x -> System.out.println(x));

/*
        JavaPairRDD<String,Integer> pair = lines.flatMapToPair(line ->{
            String[] words = line.split(" ");
            ArrayList<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String, Integer>>();
            for(String word : words){
                list.add(new Tuple2<String,Integer>(word,1));
            }
            return list.iterator();
        });


        /*
        JavaRDD<String>  words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
*/
        /*
        JavaRDD<String>  words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
*/
/*
        JavaRDD<String[]> words = lines.map(new Function<String, String[]>() {
            @Override
            public String[] call(String v1) throws Exception {
                return v1.split(" ");
            }
        });


        words.foreach( x -> System.out.println(x));
*/


        /*
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data, 2);
        JavaPairRDD<String,Integer> pair = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] strs = s.split("\\\t");
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                for (String str : strs) {
                    Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>(str, 1);
                    list.add(tuple2);
                }
                return list.iterator();
            }
        });

        JavaPairRDD<String,Integer> res = pair.reduceByKey((x, y) -> x + y);


        JavaPairRDD<String,Integer> pair1 = lines.flatMap(line -> Arrays.asList(line.split("\\\t")).iterator())
                                                 .mapToPair(x -> new Tuple2<String, Integer>(x, 1));

        JavaPairRDD<String,Integer> res1 = pair1.reduceByKey((x, y) -> x + y);

        res1.take(3).forEach(x -> System.out.println(x._1() +" : "+x._2()));
*/
/*
        JavaRDD<Integer> lambdaDistData = lines.map(x -> x.split("\\\t").toString()).map(x -> x.length()).persist(StorageLevel.MEMORY_ONLY());
        long start = System.currentTimeMillis();
        int sum = lambdaDistData.reduce((a, b) -> a + b);
        long end = System.currentTimeMillis();
        System.out.println((end - start )  +"ms: "+sum);

        long start1 = System.currentTimeMillis();
        int sum1 = lambdaDistData.reduce((a, b) -> a + b);
        long end1 = System.currentTimeMillis();
        System.out.println((end1 - start1 )  +"ms: "+sum1);
*/
        //lambdaDistData.saveAsTextFile("C:\\Users\\chenhaolin\\Desktop\\test\\123",SnappyCodec.class);
        //lambdaDistData.saveAsTextFile("C:\\Users\\chenhaolin\\Desktop\\test\\123");

        //distData.foreach(x -> counter.add(x));
        //System.out.println(counter);
    }
}
