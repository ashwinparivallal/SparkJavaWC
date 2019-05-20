
import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;
import java.util.Iterator;
import java.lang.String;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


public class wc{
    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        // Split up into words.
        JavaPairRDD<String, Integer> counts = input.flatMap(s-> Arrays.asList(s.split(" ")).iterator())
                                .mapToPair(word -> new Tuple2<>(word, 1))
                                .reduceByKey((a, b) -> a + b);
        counts.foreach(data->{
            System.out.println(data._1() + data._2());
        });
        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile(outputFile);
    }
}