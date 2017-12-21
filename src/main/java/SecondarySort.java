import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * User:leen
 * Date:2017/10/18 0018
 * Time:12:00
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\chenhaolin\\Desktop\\sort.txt");

        JavaPairRDD<SecondarySortKey,String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] lineSplits = line.split(" ");
                SecondarySortKey key = new SecondarySortKey(Integer.valueOf(lineSplits[0]), Integer.valueOf(lineSplits[1]));
                return new Tuple2<SecondarySortKey, String>(key, line);
            }
        });

        JavaPairRDD<SecondarySortKey,String> sortedPairs = pairs.sortByKey();
        sortedPairs.foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
            public void call(Tuple2<SecondarySortKey, String> res) throws Exception {
                System.out.println(res._1().hashCode()+"\t: "+ res._2());
            }
        });


    }
}
