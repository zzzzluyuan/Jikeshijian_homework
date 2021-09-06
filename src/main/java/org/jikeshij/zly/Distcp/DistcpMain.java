package org.jikeshij.zly.Distcp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DistcpMain {
    public static List<Tuple2> fileList = new ArrayList<>();
    public static SparkConf conf = new SparkConf();

    public static void main(String[] args) throws IOException {

        conf.setMaster("local[*]");
        conf.setAppName("Spark distcp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Path s_path = new Path("/Users/chunyu/IdeaProjects/hajimei_distcp/src/test/output");
        Path t_path = new Path("/Users/chunyu/IdeaProjects/hajimei_distcp/src/test/newoutput");

        //get all files and create target folder
        checkDirectories(s_path, t_path);

        //copy files
        JavaRDD<Tuple2> rdd1 = sc.parallelize(fileList, 5);
        rdd1.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2>, Object>() {
                    @Override
                    public Iterator<Object> call(Iterator<Tuple2> tuple2Iterator) throws Exception {
                        while (tuple2Iterator.hasNext()) {
                            Tuple2<Path, Path> next = tuple2Iterator.next();
                            copyFile(next._1, next._2);
                        }
                        return null;
                    }
                }
        ).collect();
        sc.stop();
    }

    public static void copyFile(Path s_path, Path t_path) throws IOException {
        Configuration hadoopconf = new Configuration();
        FileSystem s_fs = s_path.getFileSystem(hadoopconf);
        FileSystem t_fs = t_path.getFileSystem(hadoopconf);
        FileUtil.copy(s_fs, s_path, t_fs, t_path, false, hadoopconf);

    }

    public static void checkDirectories(Path s_path, Path t_path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem s_fs = FileSystem.get(conf);
        FileSystem t_fs = FileSystem.get(conf);

        for (FileStatus fileStatus : s_fs.listStatus(s_path)) {
            if (fileStatus.isDirectory()) {
                String sub_path = fileStatus.getPath().toString().split(s_path.toString())[1];
                //if folder not exist then create,otherwise overwrite? ->hajimei todo
                t_fs.mkdirs(new Path(t_path + sub_path));
                checkDirectories(fileStatus.getPath(), new Path(t_path + sub_path));
            } else {
                //prepare fileList
                fileList.add(new Tuple2(fileStatus.getPath(), t_path));
            }

        }
    }

    public static void copyFile2(Path s_path, Path t_path) throws IOException {
        //seems not support under hdfs?
        Configuration conf = new Configuration();
        FileSystem s_fs = FileSystem.get(conf);
        FileSystem t_fs = FileSystem.get(conf);

        InputStream is = new FileInputStream(String.valueOf(s_path));
        OutputStream os = new FileOutputStream(String.valueOf(t_path));
        //copy  loop+read+write
        byte[] flush = new byte[1024];
        int len = 0;

        while (-1 != (len = is.read(flush))) {
            os.write(flush, 0, len);
        }
        os.flush();
        os.close();
    }

}