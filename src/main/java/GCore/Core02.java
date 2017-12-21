package GCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * User:leen
 * Date:2017/12/8 0008
 * Time:10:29
 */
public class Core02 implements Serializable {
    public static void main(String[] args) {
        sortByKey();

/*
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("c", "d", "e"));
        String res = rdd1.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 +"-"+v2;
            }
        });
        System.out.println(res);

/*
        JavaRDD<String> res = rdd1.subtract(rdd2,2);
        res.foreach(x -> System.out.print(x+" "));


        /*
        JavaRDD<String> res = rdd1.intersection(rdd2);
        res.foreach(x -> System.out.print(x+" "));

        JavaPairRDD<String,String> res = rdd1.cartesian(rdd2);
        res.foreach(x -> System.out.print(x+" "));
/*
        JavaRDD<String> res = rdd1.distinct(2);
        res.foreach(x -> System.out.print(x+" "));
/*
        JavaRDD<String> res = rdd1.union(rdd2);
        res.foreach(x -> System.out.print(x+" "));
        */
    }

    public static void aggregateOp() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("aggregateOp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 2);

        // 1、seqOp
        Function2<Integer, Integer, Integer> seqOp = new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return Math.max(v1, v2);
            }
        };
        //2、combOp
        Function2<Integer, Integer, Integer> combOp = new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return Math.min(v1, v2);
            }
        };

        //3、aggregate
        Integer res = rdd1.aggregate(0, seqOp, combOp);

        System.out.println(res);
    }

    public static void foldByKeyOp() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("foldByKeyOp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(Arrays.asList(
                new Tuple2<String, Integer>("a", 0),
                new Tuple2<String, Integer>("a", 2),
                new Tuple2<String, Integer>("b", 1),
                new Tuple2<String, Integer>("b", 2)), 4);
        //转pairRDD
        JavaPairRDD<String, Integer> pairRDD = JavaPairRDD.fromJavaRDD(rdd);
        //func: JFunction2[V, V, V]
        Function2<Integer, Integer, Integer> func = new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        };
        //foldByKey
        JavaPairRDD<String, Integer> res = pairRDD.foldByKey(0, func);
        //打印结果
        res.foreach(x -> System.out.println(x));
    }


    public static void groupByKey() {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 60),
                new Tuple2<String, Integer>("class1", 60),
                new Tuple2<String, Integer>("class2", 50)
        );

        //JavaPairRDD<String,Integer> pairRDD = sc.parallelizePairs(scoreList);
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(scoreList);
        JavaPairRDD<String, Integer> pairRDD = JavaPairRDD.fromJavaRDD(rdd);


        JavaPairRDD<String, Iterable<Integer>> resRdd = pairRDD.groupByKey();
        /*
        resRdd.foreach(x -> {
            System.out.println("key:" + x._1());
            Iterator<Integer> iterator = x._2().iterator();
            while (iterator.hasNext()){
                System.out.println(iterator.next());
            }
        });
        */

        //只有在预期结果数据很小的情况下，才应该使用此方法collectAsMap(),
        //因为所有的数据都将拉回driver的内存中。
        Map<String, Iterable<Integer>> map = resRdd.collectAsMap();
        for (String k : map.keySet()) {
            System.out.println("key:" + k);
            Iterator<Integer> iterator = map.get(k).iterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        }

        sc.close();
    }


    public static void sortByKey()  {
        SparkConf conf = new SparkConf().setAppName("sortBy").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("1", 90),
                new Tuple2<String, Integer>("2", 60),
                new Tuple2<String, Integer>("21", 60),
                new Tuple2<String, Integer>("3", 50)
        );

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(scoreList);

        //自定义比较器
        Comparator<String> comparator = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
            }
        };

        JavaPairRDD<String, Integer> res = pairRDD.sortByKey(comparator);

        res.foreach(x -> System.out.print(x + " "));
        sc.close();
    }

    private static void sortBy() {
        SparkConf conf = new SparkConf().setAppName("sortBy").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numList = Arrays.asList(1, 5, 3, 5, 6, 7, 0, 1, 10);
        JavaRDD<Integer> numRdd = sc.parallelize(numList);

        Function<Integer, Integer> fun1 = new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        };

        JavaRDD<Integer> res = numRdd.sortBy(fun1, false, 1);
        res.foreach(x -> System.out.print(x + " "));

        sc.close();
    }

}
