package org.jikeshij.zly.Distcp;

import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.Iterator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import java.io.Serializable;

public class SparkDistCp implements Serializable {

    public static final String APP_NAME = "Spark Distribute Copy";
    public static final String LOCAL = "local";

    public static void main(String[] args) throws IOException, ParseException {

        // 解析参数
        OptionParser op = new OptionParser();
        op.parseOption(args);
        final boolean ignoreFlag = op.isIgnoreFlag();
        int maxConcurrency = op.getMaxConcurrency();
        String destPathString = op.getDestPath();

        // 初始化SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName(APP_NAME)
                .master(LOCAL)
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Path sourcePath = new Path(op.getSourcePath());
        Path destPath = new Path(destPathString);

        List<Tuple2<Path, Path>> fileList = new ArrayList<>();
        List<FileStatus> folderList = new ArrayList<>();

        checkDirectories(sourcePath, sourcePath, destPath, spark.sparkContext(), fileList, folderList);

        List fileStringList = fileList.stream().map(i -> new Tuple2(i._1.toString(), i._2.toString())).collect(Collectors.toList());

        SerializableHadoopConfiguration scf = new SerializableHadoopConfiguration(spark.sparkContext().hadoopConfiguration());

        // 设置并发数, maxConcurrency
        JavaPairRDD<String, String> fileListRDD = new JavaSparkContext(spark.sparkContext()).parallelizePairs(fileStringList, maxConcurrency);

        JavaRDD<String> processedRDD = fileListRDD.mapPartitions(
                (FlatMapFunction<Iterator<Tuple2<String, String>>, String>) stringIterator -> {
                    List<String> resultList = new ArrayList<>();
                    while (stringIterator.hasNext()) {
                        Tuple2<String, String> nextTuple = stringIterator.next();

                        try {
                            FileSystem fsInner = new Path(nextTuple._1).getFileSystem(scf.get());
                            Boolean flag = FileUtil.copy(fsInner, new Path(nextTuple._1), fsInner, new Path(nextTuple._2), false, scf.get());
                            if (flag) {
                                resultList.add("From " + nextTuple._1 + " ---> " + nextTuple._2 + " Success !");
                            }
                        } catch (Exception e) {
                            // 根据ignoreFlag的值确定是否忽略错误
                            if (ignoreFlag == true) {
                                System.out.println(nextTuple._1 + " File Copy Failed !");
                            } else {
                                System.exit(1);
                            }
                        }
                    }
                    return resultList.iterator();
                }
        );
        processedRDD.foreach(item->System.out.println(item));

    }

    // 生成源目录和目标目录的Tuple
    private static void checkDirectories(Path sourcePath, Path sourcePathRoot, Path destPath, SparkContext sparkContext,
                                         List fileList, List folderList) throws IOException {

        FileSystem fs = sourcePath.getFileSystem(sparkContext.hadoopConfiguration());
        FileStatus[] files = fs.listStatus(sourcePath);

        for (FileStatus f : files) {
            String subPath = f.getPath().toString().split(sourcePathRoot.toString())[1];
            String pathToCreate = destPath.toString() + subPath;
            if (f.isDirectory()) {
                folderList.add(f);
                fs.mkdirs(new Path(pathToCreate));
                checkDirectories(f.getPath(), sourcePathRoot, destPath, sparkContext, fileList, folderList);
            } else if (f.isFile()) {
                fileList.add(new Tuple2(f.getPath(), new Path(pathToCreate)));
            }
        }
    }
}