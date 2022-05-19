package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object D02_默认并行度策略应证 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("随便")
    conf.set("spark.default.parallelism","4")

    val sc = new SparkContext(conf)

    // 没有传入分区数，底层就走默认并行度计算策略来得到默认分区数

    // 对应local运行模式：  LocalSchedulerBackend.defaultParallelism()
    // 核心逻辑： scheduler.conf.getInt("spark.default.parallelism", totalCores)

    // 对应分布式集群运行模式：  CoarseGrainedSchedulerBackend.defaultParallelism()
    // 核心逻辑： conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))

    val rdd: RDD[Int] = sc.parallelize(1 to 100000)
    println(rdd.partitions.size)  //   4



    sc.stop()
  }

}
