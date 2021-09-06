package org.jikeshij.zly.index;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InvertedIndexMain {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Spark inverted index");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = "/xxxx/src/test/input";//args[0]
        String outputPath = "/xxxx/src/test/output";//args[1]

        //read data -> Array[(String,String)] eg: Array[(hdfs://xxx/1.txt , what is it),(,)...]
        JavaPairRDD<String, String> file = sc.wholeTextFiles(inputPath);

        //extract file name and data
        JavaPairRDD<String, String> lines = file.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> fileTuple2) throws Exception {
                String[] url_split = fileTuple2._1.split("/");//split file path
                String fileName = url_split[url_split.length - 1];//get file name + extension
                return new Tuple2<String, String>(fileName, fileTuple2._2);
            }
        });

        //flatmap
        JavaRDD<Tuple2<String, String>> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> line) {
                String[] wordArr = line._2.split(" ");//get each word
                List<Tuple2<String, String>> wordList = new ArrayList<Tuple2<String, String>>();
                for (String word : wordArr) {
                    //for each word, new a tuple2 with word and original file name
                    wordList.add(new Tuple2<String, String>(word, line._1));
                }
                return wordList.iterator();
            }
        });

        //map for reduce
        JavaPairRDD<Tuple2<String, String>, Integer> wordMapForReduce = words.mapToPair(new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Tuple2<>(stringStringTuple2, 1);
            }
        });

        JavaPairRDD<Tuple2<String, String>, Integer> wordCount = wordMapForReduce.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer1, Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });

        JavaPairRDD<String, Tuple2<String, Integer>> formatResult = wordCount.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                String word = tuple2IntegerTuple2._1._1;
                String path = tuple2IntegerTuple2._1._2;
                Integer fre = tuple2IntegerTuple2._2;
                return new Tuple2<>(word, new Tuple2<>(path, fre));
            }
        });

        formatResult.groupByKey().sortByKey().saveAsTextFile(outputPath);
        sc.stop();
    }
}