package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

object Denpendency生成验证2 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("Denpendency生成验证2")

    val rdd1: RDD[String] = sc.parallelize(Seq("a a a a a b c", "a c c a b c"))

    val rdd2: RDD[String] = rdd1.flatMap(s => s.split("\\s+"))

    val rdd3: RDD[(String, Int)] = rdd2.map(w => (w, 1))

    // rdd4 宽依赖于  rdd3
    val rdd4: RDD[(String, Int)] = rdd3.partitionBy(new HashPartitioner(2))

    // rdd5 窄依赖于 rdd4
    val rdd5: RDD[(String, Int)] = rdd4.reduceByKey(new HashPartitioner(2),_ + _ )

    rdd2.count()

    rdd5.saveAsTextFile("data/deepin/wcoutput")


    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
