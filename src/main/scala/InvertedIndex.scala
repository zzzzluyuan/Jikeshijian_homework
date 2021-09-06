import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val master: String = {
      if (args.length > 0) args.apply(0)
      "local[*]"
    }
    sparkConf.setMaster(master).setAppName("InvertedIndex")
    val sc = new SparkContext(sparkConf)

    //读取文件目录，其中包含若干个文件
    val srcFile: String = {
      if (args.length > 1) args.apply(1)
      System.getProperty("user.dir")+"\\Users\\zhangluyuan\\Downloads\\Jikeshijan\\src\\main\\resources"
    }
    val outputFile: String = {
      if (args.length > 2) args.apply(2)
      srcFile + "-output-" + System.currentTimeMillis()
    }
    println("input file is:" + srcFile)
    println("outputFile is :" + outputFile)

    //
    //
    //
    val rdd: RDD[String] = sc.textFile("file:///" + srcFile)
    //每个文件按行拆分后再按单词拆分，并形成(单词、文件序号)的元组集合
    val rdd2 = rdd.mapPartitionsWithIndex((fileIndex, partition) => partition.flatMap(line => line.split(" ")).map(word => (word, fileIndex)));
    //    rdd2.cache()
    //    println("print rdd2")
    //    rdd2.foreach(println)

    // 根据(word,fileIndex)元组的word进行group分组,每个word对应1个元组：(word,List<(word,fileIndex)>)
    val rdd3 = rdd2.groupBy(wordIndexTuple => wordIndexTuple._1)
    //    rdd3.cache()
    //    println("===============print rdd3")
    //    rdd3.foreach(println)

    //对分组后的每个word里的List<(word,fileIndex)>进行处理转换
    val rdd4 = rdd3.map(sameWordTuple =>
      (sameWordTuple._1, //得到word
        sameWordTuple._2.map(innerTuple => innerTuple._2) //得到fileIndex
          .groupBy(fileIndex => fileIndex) //相同fileIndex的再进行分组
          .map(
            indexTuple => (indexTuple._1, indexTuple._2.size) //得到每个文件index及这个index里word出现的次数
          )
      )
    ).sortBy(wordCntTuple => wordCntTuple._1) //再按word排序
    //    rdd4.cache()
    //    println("===============print rdd4")
    //    rdd4.foreach(println)
    rdd4.repartition(1).saveAsTextFile("file:///" + outputFile)
    sc.stop()
  }

}
