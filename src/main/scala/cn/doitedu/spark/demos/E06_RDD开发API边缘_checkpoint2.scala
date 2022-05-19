package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{CheckPointLoader, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * checkpoint,就是把RDD持久化
 *
 */
object E06_RDD开发API边缘_checkpoint2 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("checkpoint演示2")

    // 加载之前checkpoint好的RDD3
    val rdd3: RDD[(String, Int)] = CheckPointLoader.load(sc,"data/checkpoint/59a480a8-5fa3-4af1-a330-0abf35cd5fbe/rdd-2")

    rdd3.foreach(println)


    sc.stop()
  }
}
