package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B06_RDD算子_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.parallelize(Seq("a", "b", "c", "d", "e", "f", "g"), 2)

    // idx是元素所属的rdd分区索引号
    rdd.mapPartitionsWithIndex((idx,iter)=>{

      iter.map(ele=>(idx,ele))

    }).foreach(println)

    sc.stop()

  }

}
