package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B09_RDD算子_groupByKey {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a",2), ("b",1),("a",3),("b",4),("c",1),("a",6),("b",6)), 2)

    // 分组
    val rdd2: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    rdd2.foreach(println)


    println("-------------肾上腺激素要激发------------------")

    // 补充需求：将相同key的value累加
    val rdd3 = rdd2.map(tp=>{
      (tp._1,tp._2.sum)
    })

    rdd3.foreach(println)


    sc.stop()

  }

}
