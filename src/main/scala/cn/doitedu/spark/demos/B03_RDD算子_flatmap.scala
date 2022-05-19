package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B03_RDD算子_flatmap {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("集合映射RDD")

    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.parallelize(Seq("a b c", "b b c", "c c p"))

    val rdd1: RDD[Array[String]] = rdd.map(s => s.split("\\s+"))
    rdd1.foreach(arr=>println(arr.mkString("[",",","]")))

    println("--------------华丽的分割线-------------")

    val rdd2: RDD[String] = rdd.flatMap(s => s.split("\\s+"))
    rdd2.foreach(println)

    println("--------------华丽的分割线-------------")


    val rdd3: RDD[Array[Int]] = sc.parallelize(Seq(
      Array(1,2,3),
      Array(3,4,5),
      Array(2,6,8)))
    // 压平
    val rdd4: RDD[Int] = rdd3.flatMap(arr=>arr)

    println("--------------华丽的分割线-------------")

    val rdd5 : RDD[List[Array[Int]]]= sc.parallelize(Seq(
      List(Array(1,2),Array(2,3)),
      List(Array(2,2),Array(4,3)),
      List(Array(10,2),Array(2,30)),
      List(Array(51,2),Array(6,3)),
    ))

    val rdd6 :RDD[Array[Int]]= rdd5.flatMap(lst=>lst)
    /** rdd6
     * Array(1,2)
     * Array(2,3)
     * Array(2,2)
     * Array(4,3)
     * Array(10,2)
     * Array(2,30)
     * Array(51,2)
     * Array(6,3)
     */

    val rdd7 :RDD[Int]= rdd6.flatMap(arr=>arr)
    /**
     * 1
     * 2
     * 2
     * 3
     * 2
     * 2
     * ....
     */
    rdd7.foreach(println)


    println("--------------华丽的分割线-------------")

    val rdd8 = sc.parallelize(Seq(
      List(Map("a"->1,"b"->2),Map("c"->3)),
      List(Map("d"->6,"c"->4),Map("x"->2))
    ))

    val rdd9:RDD[Map[String,Int]] = rdd8.flatMap(lst=>lst)



    sc.stop()

  }
}
