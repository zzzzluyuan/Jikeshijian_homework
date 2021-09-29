package distcp

import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SparkDistCp {
  /**
   * 准备目录
   */
  def checkDirectories(hdfsServer: String, srcPath: Path, targetPath: Path): ArrayBuffer[(String, String)] = {
    val hiveConf = new HiveConf
    hiveConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, hdfsServer)
    val fs = createFs(hiveConf)
    val fileList = new ArrayBuffer[(String, String)]()
    fs.listStatus(srcPath)
      .foreach(status => {
        if (status.isDirectory) {
          val subPath = status.getPath.toString.split(srcPath.toString)(1)
          println("发现目录:" + subPath)
          val tarPath = new Path(targetPath + subPath)
          println("创建目标:" + tarPath.toString)
          fs.mkdirs(tarPath)
          val fileList2 = checkDirectories(hdfsServer, status.getPath, tarPath)
          fileList.appendAll(fileList2)
        } else {
          println("发现同步文件:" + status.getPath.toString + "->" + targetPath.toString)
          fileList.append((status.getPath.toString, targetPath.toString))
        }
      })
    fileList
  }

  /**
   * 复制文件
   */
  def copyFiles(sparkConf: SparkConf, hdfsServer: String, fileList: ArrayBuffer[(String, String)], ignoreFail: Boolean, partitionNum: Int): Unit = {
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(fileList, partitionNum)
    rdd.mapPartitions(fileListIterator => {
      val copyResult = new ArrayBuffer[Boolean]()
      val hiveConf = new HiveConf
      //在partition内部构造，避免序列化问题
      hiveConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, hdfsServer)
      val fs = createFs(hiveConf)

      while (fileListIterator.hasNext) {
        val fileTuple = fileListIterator.next()
        try {
          val result = FileUtils.copy(fs, new Path(fileTuple._1), fs, new Path(fileTuple._2), false, true, hiveConf)
          println("复制文件:" + fileTuple._1 + "->" + fileTuple._2 + ",成功?" + result)
          copyResult.append(result)
        } catch {
          case e: Exception => {
            println("复制文件异常." + fileTuple._1 + "->" + fileTuple._2)
            copyResult.append(ignoreFail)
          }
        }
      }
      copyResult.iterator
    }).collect()
  }

  def createFs(hiveConf: HiveConf): FileSystem = {
    FileSystem.get(hiveConf)
  }

  def main(args: Array[String]): Unit = {
    val hdfsServer: String = {
      if (args.length > 0) args.apply(0)
      "hdfs://127.0.0.1:9000"
    }
    val srcPath: String = {
      if (args.length > 0) args.apply(0)
      "/user/zly1"
    }
    val targetPath: String = {
      if (args.length > 0) args.apply(1)
      "/user/target"
    }
    val ignoreFail: Boolean = {
      if (args.length > 0) args.apply(2).toBoolean
      true
    }
    val concurrent: Int = {
      if (args.length > 0) args.apply(3).toInt
      4
    }
    val master: String = {
      if (args.length > 4) args.apply(4)
      "local[*]"
    }
    println("start diskcopy with hdfs server:" + hdfsServer)
    println("start diskcopy with srcPath:" + srcPath)
    println("start diskcopy with targetPath:" + targetPath)
    println("start diskcopy with ignoreFail:" + ignoreFail)
    println("start diskcopy with concurrent:" + concurrent)


    println("开始检查目标目录")
    val fileList = checkDirectories(hdfsServer, new Path(srcPath), new Path(targetPath))
    println("需要复制的文件总数:" + fileList.size)
    //
    val sparkConf = new SparkConf()
    sparkConf.setMaster(master).setAppName("SparkDiskCp")
    println("开始复制文件")
    copyFiles(sparkConf, hdfsServer, fileList, ignoreFail, concurrent)
  }
}
