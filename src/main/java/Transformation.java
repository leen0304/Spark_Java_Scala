import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 * User:chenhaolin
 * Date:2017/10/10 0010
 * Time:20:35
 */
public class Transformation {
    public static void main(String[] args) {

        // map();
        // filter();
        //flatMap();
        //groupByKey();
        //reduceByKey();
        //sortByKey();
        join();
    }

    /**
     * map 算子
     */
    private static void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建集合
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        //平行化集合创建初始RDD
        JavaRDD<Integer> numRdd = sc.parallelize(list);

        //map算子是对任何的RDD都可以调用的， 在Java中接受的是Function 对象
        JavaRDD res = numRdd.map(new Function<Integer, Object>() {
            public Object call(Integer num) throws Exception {
                return num * 2;
            }
        });

        //foreach 遍历打印；调用的是VoidFunction 对象
        res.foreach(new VoidFunction() {
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
        //关闭
        sc.close();
    }

    /**
     * filter  算子
     */

    private static void filter() {
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建集合
        List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        //平行化创建RDD
        JavaRDD<Integer> numRdd = sc.parallelize(numList);
        //filter 操作  call的返回类型是 Boolean 如果想保留此元素则返回 true 否则返回false
        JavaRDD<Integer> res = numRdd.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });
        //遍历打印输出
        res.foreach(new VoidFunction<Integer>() {
            public void call(Integer res) throws Exception {
                System.out.println(res);
            }
        });

        sc.close();
    }

    /**
     * flatMap 算子
     */

    private static void flatMap() {
        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读入文件 创建RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\spark.txt", 1);
        //flatMap操作 在Java中接收 FlatMapFunction
        // 返回 Interable数组，则Arrays.asList 实现
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return  Arrays.asList(s.split(" ")).iterator();
            }
        });

        words.foreach(new VoidFunction<String>() {
            public void call(String word) throws Exception {
                System.out.println(word);
            }
        });

        sc.close();
    }

    /**
     * groupByKey 算子
     */

    private static void groupByKey() {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 60),
                new Tuple2<String, Integer>("class1", 60),
                new Tuple2<String, Integer>("class2", 50)
        );

        //平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
        JavaPairRDD<String, Integer> scoreRdd = sc.parallelizePairs(scoreList);

        //groupByKey 操作
        JavaPairRDD<String, Iterable<Integer>> resRdd = scoreRdd.groupByKey();

        //遍历打印
        resRdd.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> res) throws Exception {
                System.out.println(res._1());
                Iterator<Integer> scores = res._2().iterator();
                while (scores.hasNext()) {
                    System.out.println(scores.next());
                }
                System.out.println("-----------------------");
            }
        });

        sc.close();
    }

    /**
     * reduceByKey  算子
     */

    private static void reduceByKey() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 60),
                new Tuple2<String, Integer>("class1", 60),
                new Tuple2<String, Integer>("class2", 50)
        );

        //平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
        JavaPairRDD<String, Integer> scoreRdd = sc.parallelizePairs(scoreList);

        //reduceByKey 操作  接收的参数是 Function2 其中第一个与第二个泛型是的传入两个参数的类型，第三个泛型是输出结果的类型；
        JavaPairRDD<String, Integer> resRdd = scoreRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //打印输出
        resRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1() + ":" + tuple2._2());
            }
        });

        sc.close();
    }

    /**
     * sortByKey 算子
     * <p>
     * 在Java中没有 sortBy 算子
     */

    private static void sortByKey() {
        SparkConf conf = new SparkConf().setAppName("sortBy").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class3", 40),
                new Tuple2<String, Integer>("class1", 60),
                new Tuple2<String, Integer>("class2", 50)
        );

        //平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
        JavaPairRDD<String, Integer> scoreRdd1 = sc.parallelizePairs(scoreList);

        // 将元组对中的K与V位置互换，方便后边排序
        //用到算子 mapToPair 调用方法PairFunction
        JavaPairRDD<Integer, String> scoreRdd = scoreRdd1.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2(), tuple2._1());
            }
        });

        //排序
        JavaPairRDD<Integer, String> resRdd = scoreRdd.sortByKey(false);
        //打印输出
        resRdd.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            public void call(Tuple2<Integer, String> res) throws Exception {
                System.out.println(res._2() + ":" + res._1());
            }
        });
        sc.close();
    }

    /**
     * join 算子操作
     */

    private static void join() {
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建List集合
        List<Tuple2<Integer, String>> studentsList = Arrays.asList(
                new Tuple2<Integer, String>(1001, "xiaoming"),
                new Tuple2<Integer, String>(1002, "jack"),
                new Tuple2<Integer, String>(1003, "xiaohua")
        );
        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<Integer, Integer>(1003, 88),
                new Tuple2<Integer, Integer>(1001, 66),
                new Tuple2<Integer, Integer>(1001, 22)
        );

        //parallelizePairs 平行化JavaPairRDD
        JavaPairRDD<Integer, String> studentsRdd = sc.parallelizePairs(studentsList);
        JavaPairRDD<Integer, Integer> scoresRdd = sc.parallelizePairs(scoresList);

        //join 操作
        JavaPairRDD<Integer,Tuple2<String,Integer>> ss = studentsRdd.join(scoresRdd);
        //遍历打印
        ss.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> ss) throws Exception {
                System.out.println("学号："+ss._1());
                System.out.println("姓名："+ss._2()._1());
                System.out.println("分数："+ss._2()._2());
                System.out.println("-----------------------");
            }
        });

//        // leftOuterJoin 操作
//        JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> ss = studentsRdd.leftOuterJoin(scoresRdd);
//        ss.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Optional<Integer>>>>() {
//            public void call(Tuple2<Integer, Tuple2<String, Optional<Integer>>> ss) throws Exception {
//                System.out.println("学号：" + ss._1());
//                System.out.println("姓名：" + ss._2()._1());
//                Set<Integer> set = ss._2()._2().asSet();
//                Iterator<Integer> iterator = set.iterator();
//                while (iterator.hasNext()){
//                    System.out.println("分数：" + iterator.next());
//                }
//                System.out.println("-----------------------");
//            }
//        });

//        //fullOuterJoin 操作
//       JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> ss =  studentsRdd.fullOuterJoin(scoresRdd);
//
//        ss.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<String>, Optional<Integer>>>>() {
//            public void call(Tuple2<Integer, Tuple2<Optional<String>, Optional<Integer>>> ss) throws Exception {
//                System.out.println("学号：" + ss._1());
//                System.out.println("姓名：" + ss._2()._1().get());
//                Set<Integer> set = ss._2()._2().asSet();
//                Iterator<Integer> iterator = set.iterator();
//                while (iterator.hasNext()){
//                    System.out.println("分数：" + iterator.next());
//                }
//                System.out.println("-----------------------");
//            }
//        });
        sc.close();
    }

}






























