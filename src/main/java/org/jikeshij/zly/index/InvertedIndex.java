package org.jikeshij.zly.index;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class InvertedIndex {
    public static final String APP_NAME = "Inverted Index Demo";
    public static final String LOCAL = "local";

    public static void main(String[] args) {
        // Spark 环境初始化
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(LOCAL);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String filePath = args[0];

        // 初始化FileHandler
        FileHandler fh = new FileHandler();

        // 首先转换为JavaPairRDD, 生成倒排索引
        JavaPairRDD<String, String> invertedWords = jsc.parallelizePairs(fh.getFileContentList(filePath));

        // 生成 ((单词，路径),1) 的格式
        JavaPairRDD<Tuple2<String, String>, Integer> mapWithOne = invertedWords.mapToPair(x->new Tuple2<>(new Tuple2<>(x._1,x._2),1));

        // 生成 以 (单词，路径）为key，利用reduceByKey加和统计
        JavaPairRDD<Tuple2<String, String>,Integer> groupWords = mapWithOne.reduceByKey(Integer::sum);

        // 从 ((单词,路径),词频）形式转化为 (单词，(路径，词频)) 形式
        JavaPairRDD<String,Tuple2<String,Integer>> mapWords = groupWords.mapToPair(x->new Tuple2<>(x._1._1, new Tuple2<>(x._1._2,x._2)));

        // groupByKey，按照单词合并最终结果
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupWordsOnly = mapWords.groupByKey().sortByKey();

        // 打印最终结果
        for(Tuple2 <String, Iterable<Tuple2<String, Integer>>> t: groupWordsOnly.collect()){
            System.out.println("\""+t._1+"\""+" : "+t._2.toString().replace("[","{").replace("]","}"));
        }
    }
}