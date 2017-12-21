import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:17:41
 */
public class GroupTopN {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("GroupTopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\group.txt");

        //拆分为JavaPairRDD<String, Integer>
        JavaPairRDD<String, Integer> cs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
            }
        });

        //根据Key分组
        JavaPairRDD<String, Iterable<Integer>> csPairs = cs.groupByKey();
        //根据Key排序，升序
        JavaPairRDD<String, Iterable<Integer>> csPairs1 = csPairs.sortByKey();
        //遍历取出Top3
        csPairs1.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> csPair) throws Exception {
                String name = csPair._1();
                Iterator<Integer> ite = csPair._2().iterator();
                Integer[] res = new Integer[3];
                //排序，取出Top3
                while (ite.hasNext()) {
                    Integer score = ite.next();
                    for (int i = 0; i < 3; i++) {
                        if (res[i] == null) {
                            res[i] = score;
                            break;
                        } else if (res[i] < score) {
                            for (int j = 2; j > i; j--) {
                                res[j] = res[j - 1];
                            }
                            res[i] = score;
                            break;
                        }
                    }
                }
                System.out.print(name + ":");
                for (int i = 0; i < res.length; i++) {
                    System.out.print(res[i] + "\t");
                }
                System.out.println();
            }
        });

        sc.close();
    }
}






