package GCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * User:leen
 * Date:2017/12/16 0016
 * Time:15:21
 */
public class Core05 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("mapPartitions");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        //mapPartitionsToPair
        JavaPairRDD<Integer, Integer> rdd01 = rdd1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Integer>, Integer, Integer>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<Integer> i) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
                while (i.hasNext()) {
                    int num = i.next();
                    list.add(new Tuple2<Integer, Integer>(num, num * 2));
                }
                return list.iterator();
            }
        });

        rdd01.foreach(x -> System.out.println(x));

        //mapPartitions
        JavaRDD<Tuple2<Integer, Integer>> rdd02 = rdd1.mapPartitions(new FlatMapFunction<Iterator<Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<Integer> i) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
                while (i.hasNext()) {
                    int next = i.next();
                    list.add(new Tuple2<Integer, Integer>(next, next * 2));
                }

                return list.iterator();
            }
        });

        rdd02.foreach(x -> System.out.println(x));

        //mapPartitionsWithIndex
        JavaRDD<Tuple2<Integer, ArrayList<Integer>>> rdd03 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, ArrayList<Integer>>>>() {
            @Override
            public Iterator<Tuple2<Integer, ArrayList<Integer>>> call(Integer v1, Iterator<Integer> v2) throws Exception {
                HashMap<Integer, ArrayList<Integer>> map = new HashMap<Integer, ArrayList<Integer>>();
                ArrayList<Integer> list = null;
                while (v2.hasNext()) {
                    Integer next = v2.next();
                    if (map.containsKey(v1) && list != null) {
                        ArrayList<Integer> tmpList = map.get(v1);
                        tmpList.add(next);
                        map.put(v1, tmpList);
                    } else {
                        list = new ArrayList<Integer>();
                        list.add(next);
                        map.put(v1, list);
                    }
                }

                Iterator<Integer> iterator = map.keySet().iterator();
                HashSet<Tuple2<Integer, ArrayList<Integer>>> set = new HashSet<Tuple2<Integer, ArrayList<Integer>>>();
                while (iterator.hasNext()) {
                    int next = iterator.next();
                    set.add(new Tuple2<Integer, ArrayList<Integer>>(next, map.get(next)));
                }

                return set.iterator();
            }
        }, false);

        rdd03.foreach(x -> System.out.println(x));

        JavaRDD<java.util.List<Integer>> rdd04 = rdd1.glom();

        rdd04.foreach(x -> System.out.println(x));


    }
}
