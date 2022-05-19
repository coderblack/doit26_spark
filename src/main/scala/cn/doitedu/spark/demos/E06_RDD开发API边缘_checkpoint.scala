package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * checkpoint,就是把RDD持久化
 *
 */
object E06_RDD开发API边缘_checkpoint {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("checkpoint演示")
    sc.setCheckpointDir("data/checkpoint/")

    val rdd1: RDD[Int] = sc.parallelize(1 to 10000)

    val rdd2: RDD[(String, Int)] = rdd1.map(i => (i + "", i))

    val rdd3: RDD[(String, Int)] = rdd2.map(tp => (tp._1, tp._2 * 10))

    rdd3.cache()
    // 还可以把rdd3持久化到HDFS
    /**
     * 如果你觉得，你的这个rdd3非常宝贵，一旦丢失需要重算的话，代价太高
     * 那么，就可以将rdd3执行checkpoint来持久化保存
     */
    rdd3.checkpoint() // 会额外新增一个job来计算rdd3的数据并存储到HDFS

    rdd3.reduceByKey(_ + _).count()

    val rdd4 = rdd3.map(tp => (tp._1 + "_a", Math.max(tp._2, 100)))
    val rdd5 = rdd4.groupByKey()
    val rdd6 = rdd5.mapValues(iter => iter.max)
    rdd6.count()

    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
