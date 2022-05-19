package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B10_RDD算子_reduceByKey {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a",2), ("b",1),("a",3),("b",4),("c",1),("a",6),("b",6)), 2)

    // reduceByKey
    val rdd2: RDD[(String, Int)] = rdd.reduceByKey((merge, elem) => merge + elem)

    // 如下这个写法得到的结果与上面的reduceByKey完全相同
    val rdd3 = rdd.groupByKey().map(tp=>(tp._1,tp._2.sum))


    /**
     * 如果要实现上面的同样的需求，用 reduceByKey 比 “groupByKey后再聚合” 效率更高
     *
     */



    rdd2.foreach(println)



    sc.stop()

  }

}
