import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.storage.StorageLevel;


/**
 * User:leen
 * Date:2017/12/5 0005
 * Time:17:08
 */
public class JavaKryoSerializer {

    static class Tmp1 implements java.io.Serializable {
        public int total_;
        public int num_;
    }

    public static class toKryoRegistrator implements KryoRegistrator {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(Tmp1.class, new FieldSerializer(kryo, Tmp1.class));
        }
    }


    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("JavaKryoSerializer").setMaster("local");
        //使用Kryo序列化库，如果要使用Java序列化库，需要把该行屏蔽掉
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //在Kryo序列化库中注册自定义的类集合，如果要使用Java序列化库，需要把该行屏蔽掉
        conf.set("spark.kryo.registrator", toKryoRegistrator.class.getName());
        //RDD压缩
        conf.set("spark.rdd.compress", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://leen:8020/user/hive/warehouse/tools.db/dwd_ev_pub_user_act_app/000000_0");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split("\\\t")).iterator();
            }
        });


        JavaRDD<Integer> wordsLength = words.map(new Function<String, Integer>() {
            @Override
            public Integer call(String word) {
                return word.length();
            }
        });

        JavaRDD<Tmp1> res = wordsLength.map(new Function<Integer, Tmp1>() {
            @Override
            public Tmp1 call(Integer x) {
                //只是为了将rdd4中的元素类型转换为Tmp1类型的对象，没有实际的意义
                Tmp1 a = new Tmp1();
                a.total_ += x;
                a.num_ += 1;
                return a;
            }
        });

        //将rdd4以序列化的形式缓存在内存中，因为其元素是Tmp1对象，所以使用Kryo的序列化方式缓存
        res.persist(StorageLevel.MEMORY_ONLY_SER());
        System.out.println("the count is " + res.count());

        //调试命令，只是用来将程序挂住，方便在Driver 4040的WEB UI中观察rdd的storage情况
        while (true) {
        }
        //sc.stop();
    }


}
