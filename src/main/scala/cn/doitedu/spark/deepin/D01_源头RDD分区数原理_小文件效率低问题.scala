package cn.doitedu.spark.deepin

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.CombineFileInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object D01_源头RDD分区数原理_小文件效率低问题 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    // 指定运行模式
    conf.setMaster("local")
    // 指定job的名称
    conf.setAppName("三国志")

    // 构造一个spark core的编程入口
    val sc = new SparkContext(conf)

    // 加载本地文件系统中的文本数据得到起始RDD（弹性分布式数据集）
    val rdd1: RDD[String] = sc.textFile("data/battel/input/")

    // sc.textFile底层就是调用的 sc.hadoopFile，但是InputFormat写死成了TextInpuFormat
    // 我们可以自己调sc.hadoopFile，传入自己选择InputFormat(CombineFIleInputFormat)来解决大量小文件的问题
    val rdd2 = sc.hadoopFile("path", classOf[CombineFileInputFormat[LongWritable, Text]], classOf[LongWritable], classOf[Text], 2)

    // rdd2.reduceByKey(f,5)



    val rdd3: RDD[Text] = rdd2.map(tp => tp._2)






  }


}
