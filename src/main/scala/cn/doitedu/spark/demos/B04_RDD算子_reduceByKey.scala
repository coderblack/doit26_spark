package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B04_RDD算子_reduceByKey {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("集合映射RDD")

    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.parallelize(Seq(
      "a a a b",
      "c a a b",
      "c a d b d d",
      "a a a b"
    ))

    // 将上面的rdd，做转换，得到如下数据结构的RDD
    // (a,1)
    // (b,1)
    // .....

    /*val rdd2: RDD[Array[String]] = rdd.map(s => s.split("\\s+"))
    val rdd3: RDD[String] = rdd2.flatMap(arr => arr)*/

    // 步骤1
    val rddWords: RDD[String] = rdd.flatMap(s=>s.split("\\s+"))
    /**
     * a
     * a
     * a
     * b
     * c
     * a
     * a
     * b
     * ......
     */

    // 步骤2: 将上面rdd，变成 (单词,1)的rdd
    val rddPair: RDD[(String, Int)] = rddWords.map(w=>(w,1))

    // 步骤1+步骤2 ，在一个flatmap中实现
    // val rddPair2: RDD[(String, Int)] = rdd.flatMap(s=>s.split("\\s+").map(w => (w, 1)))

    /**
     * (a,1)
     * (a,1)
     * (a,1)
     * (b,1)
     * (c,1)
     * (a,1)
     * (a,1)
     * (b,1)
     * ......
     */

    // 按单词分组，累加元组中的value(1)
    //rddPair.reduceByKey((聚合值,元素值)=>新的聚合值)
    // reduceByKey ： 按照相同的key进行分组聚合，聚合的运算逻辑由你写
    // val res: RDD[(String, Int)] = rddPair.reduceByKey((merge, elem) => merge + elem)
    // val res: RDD[(String, Int)] = rddPair.reduceByKey((x, y) => x+y)
    val res: RDD[(String, Int)] = rddPair.reduceByKey(_+_)
    res.foreach(println)

    sc.stop()


  }


}
