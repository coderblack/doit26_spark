package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object B13_RDD算子_Coalesce {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 2), ("a", 3), ("b", 1), ("b", 4), ("c", 1), ("a", 6), ("b", 6), ("d", 6)), 3)
    println(rdd.partitions.size)

    //  coalesce可以改变RDD的分区数
    //  当 shuffle=true时，可以将原来的rdd的分区数变多和变少
    //  当 shuffle=false时，只能将原来的rdd的分区数变少（哪怕给一个更多的分区数，实际结果只会保持原来的分区数）
    val rdd2: RDD[(String, Int)] = rdd.coalesce(6, false)  // 不会改变原来的分区数
    val rdd3: RDD[(String, Int)] = rdd.coalesce(6, true)  // 可以改变原来的分区数

    println(rdd2.partitions.size)
    println(rdd3.partitions.size)


    println("-------------------------------分割线---------------------")


    // repartition的实质就是调用了coalesce
    // coalesce(numPartitions, shuffle = true)  // shuffle被设置成了true
    // 所以，repartition既可以将分区数改大，也可以将分区数改小 （repartition一定会伴随着shuffle的发生）
    val rdd4: RDD[(String, Int)] = rdd.repartition(10)
    val rdd5: RDD[(String, Int)] = rdd.repartition(2)
    println(rdd4.partitions.size)
    println(rdd5.partitions.size)

    sc.stop()

  }


}
