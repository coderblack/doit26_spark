package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object B05_RDD算子_mapPartitions {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/wordcount/input")

    val rdd2: RDD[String] = rdd.flatMap(s => s.split("\\s+"))

    val rdd3 = rdd2.map(s => {
      println("f1我被执行了")
      (s, 1)
    })
    // rdd3.foreach(println)


    val rdd4 = rdd2.mapPartitions(iter => {
      println("f2被执行了")
      iter.map(s => (s, 1))
    })
    //rdd4.foreach(println)


    // mappartitions的理解测试：奇怪的代码
    rdd2.mapPartitions(iter => {
      List("haha", "heihei", "xixi").iterator
    }).foreach(println)


    /**
     * mappartitions的实际应用场景
     * 要对一份大体量的数据进行处理的时候，需要查询一个外部字典数据
     * 如果用map算子来做，就意味着对外部字典数据查询的连接创建会被高频度地执行，效率低下
     * 而用mappartitions则可以避免这个弊端： 一个分区执行一次自己的函数，那我们可以在函数中先创建一个连接，然后再去迭代处理每一行数据
     */
    //先用map算子来实现一遍，问题大大滴
    val sanguo: RDD[String] = sc.textFile("data/sanguo/input")
    val res = sanguo.map(line => {
      val arr: Array[String] = line.split(",")
      val id = arr(0).toInt

      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val stmt: PreparedStatement = conn.prepareStatement("select name,role,battel from battel where id=?")
      stmt.setInt(1, id)
      val rs: ResultSet = stmt.executeQuery()
      rs.next()
      val name = rs.getString(1)
      val role = rs.getString(2)
      val battel = rs.getInt(3)

      line + "," + name + "," + role + "," + battel
    })

    res.foreach(println)


    // 再用MapPartitions将这个实现进行优化
    val res2 = sanguo.mapPartitions(iter=>{
      // 先创建连接（这段代码只会在一个分区上执行一次）
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val stmt: PreparedStatement = conn.prepareStatement("select name,role,battel from battel where id=?")

      // 再去迭代数据进行处理
      iter.map(line=>{
        val arr: Array[String] = line.split(",")
        val id = arr(0).toInt
        stmt.setInt(1, id)

        val rs: ResultSet = stmt.executeQuery()
        rs.next()
        val name = rs.getString(1)
        val role = rs.getString(2)
        val battel = rs.getInt(3)

        line + "," + name + "," + role + "," + battel
      })
    })
    res2.foreach(println)




    sc.stop()

  }
}
