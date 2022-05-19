package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B08_RDD算子_distinct {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.parallelize(Seq("a", "b", "c", "a","b","c","d", "d", "f", "g"), 2)

    // 去重
    val rdd2: RDD[String] = rdd.distinct()
    rdd2.foreach(println)



    sc.stop()

  }

}
