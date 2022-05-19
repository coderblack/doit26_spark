package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Denpendency生成验证 {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("Denpendency生成验证")

    val rdd1: RDD[String] = sc.parallelize(Seq("a a a a b c", "a c c a b c"))

    // rdd2（MapPartitionsRDD）  窄依赖于  rdd1
    val rdd2: RDD[String] = rdd1.flatMap(s => s.split("\\s+"))

    // rdd3（MapPartitionsRDD）  窄依赖于  rdd2
    val rdd3: RDD[(String, Int)] = rdd2.map(w => (w, 1))

    // rdd4（ShuffledRDD）  宽依赖于  rdd3
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)


    rdd4.saveAsTextFile("data/deepin/wcoutput")


    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
