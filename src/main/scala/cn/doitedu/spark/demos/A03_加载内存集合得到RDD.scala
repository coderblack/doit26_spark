package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object A03_加载内存集合得到RDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("集合映射RDD")

    val sc = new SparkContext(conf)

    // 假设我造了一个普通的scala集合
    val lst = List(1,2,3,4,5,6)

    // 映射上面的list集合成为一个RDD
    val rdd: RDD[Int] = sc.parallelize(lst)

    // 打印rdd数据
    rdd.foreach(println)


    // 造一个装着句子（字符串）的Seq集合，然后映射成RDD
    val seq = Seq(
      "taoge  is a man deep as the sea",
      "taoge is a man handsome as guanxi",
      "doitedu is a school teach big data technology"
    )  // apply方法
    val rdd2: RDD[String] = sc.parallelize(seq)

    // 造一个装着Soldier对象的List集合，然后映射成RDD
    val lst2 = List(
      Soldier(1,"小花","班花",99.9),
      Soldier(2,"洪花","系花",99.9),
      Soldier(3,"兰花","校花",99.9),
      Soldier(4,"菜花","组花",99.9)
    )
    val rdd3: RDD[Soldier] = sc.parallelize(lst2)
    rdd3.foreach(println)

    // 造一个装着二元组的List集合，映射成RDD
    val lst3 = List(
      ("a",1),
      ("a",2),
      ("b",2),
      ("c",3)
    )
    val rdd4: RDD[(String, Int)] = sc.parallelize(lst3, 2)
    rdd4.foreach(println)



    sc.stop()
  }

}
