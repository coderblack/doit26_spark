package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[1]")  // local[*]
    conf.setAppName("单词计数")

    // 构造一个spark编程的入口对象
    val sc = new SparkContext(conf)
/*
    // 加载数据源
    val rdd: RDD[String] = sc.textFile("data/wordcount/input/a.txt")

    // 调用各种转换（transformation）算子
    val rdd2: RDD[String] = rdd.flatMap(s => s.split("\\s+"))
    val rdd3: RDD[(String,Int)] = rdd2.map(w=>(w,1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((a, b) => a + b)

    // 触发执行（行动action算子）
    rdd4.foreach(println)
*/
    sc.textFile("data/wordcount/input/a.txt").flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
      .foreach(println)



  }

}
