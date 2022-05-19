package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object E03_RDD开发API边缘_rdd缓存 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("RDD缓存")

    val lines: RDD[String] = sc.textFile("data/wordcount/input")

    val words: RDD[String] = lines.flatMap(_.split("\\s+"))

    val wordOnePair: RDD[(String, Int)] = words.map((_, 1))
    // cache,本质上是把RDD运算出来的结果数据进行物化
    // words.cache()
    words.persist(StorageLevel.MEMORY_ONLY)
    // rdd缓存的存储级别有如下：
       // NONE  相当于没有存储
       // DISK_ONLY  缓存到磁盘
       // DISK_ONLY_2  缓存到磁盘，2个副本
       // MEMORY_ONLY   缓存到内存
       // MEMORY_ONLY_2   缓存到内存，2个副本
       // MEMORY_ONLY_SER    缓存到内存，以序列化格式
       // MEMORY_ONLY_SER_2   缓存到内存，以序列化格式，2个副本
       // MEMORY_AND_DISK    缓存到内存和磁盘
       // MEMORY_AND_DISK_2   缓存到内存和磁盘，2个副本
       // MEMORY_AND_DISK_SER    缓存到内存和磁盘，以序列化格式
       // MEMORY_AND_DISK_SER_2   缓存到内存和磁盘，以序列化格式，2个副本
       // OFF_HEAP   缓存到堆外内存

    //  求每个单词的出现次数
    val wordcount = wordOnePair.reduceByKey(_ + _)
    wordcount.collect()

    // 将wordOnePair这个RDD中的kv对，按key分组，然后将value以字符串形式拼接
    val valuesPinjie = wordOnePair.aggregateByKey("")((u, e) => u + e + "", (u1, u2) => u1 + u2)
    valuesPinjie.saveAsTextFile("data/wordcount/output")

    // 假设程序的后续代码中，再也不用wordOnePair这个RDD了，我们还可以将之前缓存的数据清除
    wordOnePair.unpersist()

    sc.stop()
  }

}


