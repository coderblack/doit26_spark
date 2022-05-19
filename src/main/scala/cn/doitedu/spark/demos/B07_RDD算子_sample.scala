package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B07_RDD算子_sample {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.parallelize(Seq("a", "b", "c", "d", "e", "f", "g"), 2)

    // 允许样本被重复抽取，抽20%
    val sampled: RDD[String] = rdd.sample(true, 0.2)
    sampled.foreach(println)


    val sample2: Array[String] = rdd.takeSample(true, 2)
    sample2.foreach(println)


    sc.stop()

  }

}
