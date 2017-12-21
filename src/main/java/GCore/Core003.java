package GCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * User:leen
 * Date:2017/12/8 0008
 * Time:16:27
 */
public class Core003 {
    public static void main(String[] args) {
        rbk();
    }

    public static void rbk() {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        ArrayList<ScoreDetail003> scoreDetail = new ArrayList<ScoreDetail003>();
        scoreDetail.add(new ScoreDetail003("A", "Math", 98));
        scoreDetail.add(new ScoreDetail003("A", "English", 88));
        scoreDetail.add(new ScoreDetail003("B", "Math", 75));
        scoreDetail.add(new ScoreDetail003("B", "English", 78));
        scoreDetail.add(new ScoreDetail003("C", "Math", 90));
        scoreDetail.add(new ScoreDetail003("C", "English", 80));
        scoreDetail.add(new ScoreDetail003("D", "Math", 91));
        scoreDetail.add(new ScoreDetail003("D", "English", 80));

        JavaRDD<ScoreDetail003> scoreDetailRdd = sc.parallelize(scoreDetail,8);

        JavaPairRDD<String,ScoreDetail003> pairRDD = scoreDetailRdd.mapToPair(detail -> new Tuple2<String, ScoreDetail003>(detail.name,detail));

        //1、创建createCombiner：组合器函数，输入参数为RDD[K,V]中的V（即ScoreDetail对象），输出为tuple2(学生成绩，1)
        Function<ScoreDetail003, Tuple2<Float,Integer>> createCombiner = new Function<ScoreDetail003, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(ScoreDetail003 v1) throws Exception {
                return new Tuple2<Float, Integer>((float) v1.score,1);
            }
        };

        //2、mergeValue：合并值函数，输入参数为(C,V)即（tuple2（学生成绩，1），ScoreDetail对象），输出为tuple2（学生成绩，2）
        Function2<Tuple2<Float,Integer>,ScoreDetail003,Tuple2<Float,Integer>> mergeValue = new Function2<Tuple2<Float, Integer>, ScoreDetail003, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> v1, ScoreDetail003 v2) throws Exception {
                return new Tuple2<Float, Integer>(v1._1()+v2.score,v1._2()+1);
            }
        };

        //3、mergeCombiners：合并组合器函数，对多个节点上的数据合并，输入参数为(C,C)，输出为C

        Function2<Tuple2<Float,Integer>,Tuple2<Float,Integer>,Tuple2<Float,Integer>> mergeCombiners = new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> v1, Tuple2<Float, Integer> v2) throws Exception {
                return new Tuple2<Float, Integer>(v1._1()+v2._1(),v1._2()+v2._2());
            }
        };

        JavaPairRDD<String,Float> res = pairRDD.aggregateByKey(new Tuple2<Float,Integer>(1f,0),mergeValue,mergeCombiners)
                .mapToPair(x -> new Tuple2<String, Float>(x._1(),x._2()._1()/x._2()._2()));

        /**JavaPairRDD<String,Float> res = pairRDD.combineByKey(createCombiner, mergeValue, mergeCombiners, 2)
                .mapToPair(x -> new Tuple2<String, Float>(x._1(),x._2()._1()/x._2()._2()));
*/
        res.foreach(x -> System.out.println(x));


        sc.close();
    }
}
