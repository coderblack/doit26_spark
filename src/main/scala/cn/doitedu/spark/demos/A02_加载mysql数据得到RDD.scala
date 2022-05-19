package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, ResultSet}


case class Soldier(id:Int,name:String,role:String,battle:Double)

object 加载mysql数据得到RDD {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setAppName("加载数据库数据示例")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // 加载mysql中的表

    // 定义一个创建jdbc连接的函数
    val getConn = ()=>{
      DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
    }

    // 定义一个查询数据用的sql
    val sql = "select  * from battel where id>=?  and id<=?"

    // 定义一个数据映射逻辑（函数）： 映射每一行查询结果到目标数据类型
    val mapRow = (rs:ResultSet)=>{

      val id: Int = rs.getInt(1)
      val name: String = rs.getString(2)
      val role: String = rs.getString(3)
      val battle: Double = rs.getDouble(4)

      Soldier(id,name,role,battle)
    }

    // 直接构造一个JdbcRDD来映射mysql中的表数据
    val rdd: JdbcRDD[Soldier] = new JdbcRDD[Soldier](sc, getConn, sql, 0, 6, 2, mapRow)

    rdd.foreach(println)

    sc.stop()

  }


}
