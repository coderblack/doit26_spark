package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C01_RDD算子_Join {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd1: RDD[(String, Int)] = sc.parallelize(Seq(("a", 12), ("a", 13), ("b", 11), ("b", 14), ("c", 11)), 3)
    val rdd2: RDD[(String, String)] = sc.parallelize(Seq( ("a", "x"),("a","z") ,("b", "y"), ("d", "z")), 3)

    // 内连接
    // (String, (Int, String))
    //  key，左表vaule，右表value
    val joined: RDD[(String, (Int, String))] = rdd1.join(rdd2)
    joined.foreach(println)


    println("--------------------分割线-----------------")

    // 左外连接  Option有两个子类： Some(), None
    //                    key，左表vaule，Option(右表value)
    val leftJoined: RDD[(String, (Int, Option[String]))] = rdd1.leftOuterJoin(rdd2)
    leftJoined.foreach(println)

    println("--------------------分割线-----------------")

    // 右外连接
    val rightJoined: RDD[(String, (Option[Int], String))] = rdd1.rightOuterJoin(rdd2)
    rightJoined.foreach(println)


    println("--------------------分割线-----------------")

    // 全外连接
    val fulleJoined :RDD[(String,(Option[Int],Option[String]))] = rdd1.fullOuterJoin(rdd2)
    fulleJoined.foreach(println)


    sc.stop()

  }
}
