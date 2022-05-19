package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C02_RDD算子_cogroup {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd1: RDD[(String, Int)] = sc.parallelize(Seq(("a", 12), ("a", 13), ("b", 11), ("b", 14), ("c", 11)), 3)
    val rdd2: RDD[(String, String)] = sc.parallelize(Seq( ("a", "x"),("a","z") ,("b", "y"), ("d", "z")), 3)

    // 两个RDD ，协同分组
    val res: RDD[(String, (Iterable[Int], Iterable[String]))] = rdd1.cogroup(rdd2)
    res.foreach(println)



    sc.stop()

  }
}
