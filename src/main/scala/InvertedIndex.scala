import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Inverted Index")
    val sc =new SparkContext(conf)
    //SparkContext.wholeTextFiles允许你读取文件夹下所有的文件，比如多个小的文本文件， 返回文件名/内容对。
    val files =sc.wholeTextFiles("/Users/zhangluyuan/Downloads/jkshijian")
    //因为读进来的文件是路径，所以要把文件名过滤一下，前面的都去掉，只留下文件自己的名字
    val file_name_length = files.map(x=>x._1.split("/").length).collect()(0)
    val file_name_context= files.map(x=>(x._1.split("/")(file_name_length-1),x._2)).sortByKey()
    val words =file_name_context.flatMap(x=>{
      //首先要根据行来切分
      val line =x._2.split("\n")
      //每个文件生成的（文件名，单词）对用链表给串起来。注意按照下面的方法生成的list是带有一个空节点的指针，也就是说它的第一元素是null
      val list =LinkedList[(String,String)]()
      //temp相当于是一个临时的指针，用来给list插入元素的。在scala语言中，val是不可变的，var是可变的
      var temp =list
      //对每一行而言，需要根据单词拆分，然后把每个单词组成（文件名，单词）对，链到list上
      for(i <- 0 to line.length-1){
        val word =line(i).split(" ").iterator
        while (word.hasNext){
          temp.next=LinkedList[(String,String)]((x._1,word.next()))
          temp=temp.next
        }
      }
      //我们得到的list的第一个元素是null，drop函数是去掉前n个数，这里是1，我们要把第一个元素null给去掉
      val list_end=list.drop(1)
      //这个list_end是这个flatMap算子中x所要得到的东西，scala语言居然可以这样写，我也是醉了
      list_end
    }).distinct()
    //首先按照文件名排序，然后调换map的位置，将文件名串起来，再根据单词排序，最后保存
    words.sortByKey().map(x=>(x._2,x._1)).reduceByKey((a,b)=>a+";"+b).sortByKey().saveAsTextFile("/Users/zhangluyuan/Downloads/jkshijian")
    sc.stop()
  }

}
