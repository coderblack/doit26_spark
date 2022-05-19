package cn.doitedu.spark.javaspark;

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
import java.util.List;

public class JavaWordCount2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("java版wordcount");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读数据
        JavaPairRDD<String, Integer> res = sc.textFile("data/wordcount/input")
                .flatMap(s -> Arrays.asList(s.split("\\s+")).iterator())
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        List<Tuple2<String, Integer>> lst = res.collect();
        System.out.println(lst);

        sc.stop();
    }

}
