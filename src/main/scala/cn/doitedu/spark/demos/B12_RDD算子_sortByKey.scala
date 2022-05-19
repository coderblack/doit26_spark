package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B12_RDD算子_sortByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a",2),("a",3), ("b",1),("b",4),("c",1),("a",6),("b",6),("d",6)), 2)

    // spark中的sortByKey是全局排序的
    // 底层的shuffle分区器用的是： RangePartitioner
    val rdd2: RDD[(String, Int)] = rdd.sortByKey()

    //rdd2.saveAsTextFile("data/sortbykey/output")

    Thread.sleep(Long.MaxValue)
    sc.stop()
  }

}
