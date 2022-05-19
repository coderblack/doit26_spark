package cn.doitedu.spark.demos

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.CombineFileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object A01_加载文件数据源得到RDD {

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

    //
    sc.hadoopFile("path",classOf[CombineFileInputFormat[LongWritable,Text]],classOf[LongWritable],classOf[Text],2)



    // 打印rdd1中的元素
    rdd1.foreach(x => println(x))

    println("------------分割线--------------------")


    // 加载本地文件系统中的sequence文件得到起始RDD
    val rdd2: RDD[(Int, String)] = sc.sequenceFile("data/seq/input", classOf[Int], classOf[String])
    rdd2.foreach(x => println(x))

    println("------------分割线--------------------")

    // 加载HDFS文件系统中的文本文件
    val rdd3: RDD[String] = sc.textFile("hdfs://doit01:8020/wordcount/input")
    val arr: Array[String] = rdd3.take(10)
    arr.foreach(x=>println(x))



  }
}
