package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

object D04_shuffle算子不一定产生shuffle {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("shuffle算子是否产生shuffle")

    val rdd1 = sc.parallelize(Seq(
      ("a",2),
      ("a",1),
      ("b",3),
      ("c",2),
      ("a",5)
    ))

    val rdd2 = sc.parallelize(Seq(
      ("a",2),
      ("b",1),
      ("b",3),
      ("c",2),
      ("c",5)
    ))


    // rdd1.join(rdd2)  肯定有shuffle

    // rdd1.reduceByKey(_+_)   肯定有shuffle

    val rdd12: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(2))
    val rdd13 = rdd12.reduceByKey(_+_)  // 没有shuffle，因为rdd12的分区器是HashPartioner(2)，而此算子底层传入的分区器就是父RDD的这个HashPartioner(2)
    rdd13.count()


    val rdd14: RDD[(String, Int)] = rdd1.repartition(2)
    val rdd15 = rdd14.reduceByKey(_+_)  // ？？？
    rdd15.count()


    /**
     * 关于join算子的shuffle验证
     */

    val joined1 = rdd1.join(rdd2)  // 有shuffle
    joined1.count()

    val rdd_a: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(2))
    val rdd_b: RDD[(String, Int)] = rdd2.partitionBy(new HashPartitioner(2))
    val joined2 = rdd_a.join(rdd_b,new HashPartitioner(2))  // 没有shuffle
    joined2.count()

    val joined3 = rdd_a.join(rdd_b)  // 没有shuffle，因为算子帮我穿了 HashPartitioner(2)
    joined3.count()

    val joined4 = rdd_a.join(rdd_b,new HashPartitioner(3))  //  有shuffle，因为父RDD分区器和数  != 子RDD分区器数
    joined4.count()


    Thread.sleep(Long.MaxValue)
    sc.stop()

  }

}
