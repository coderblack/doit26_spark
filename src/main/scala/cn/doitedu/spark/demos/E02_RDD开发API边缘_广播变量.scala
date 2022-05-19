package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-11-25
 * @desc 广播变量主要应用于“需要进行map端join”的场合
 * 就是把一份小体量的数据，直接让每个executor持有一份拷贝，在task的计算逻辑中直接可用
 * 而不用通过两个rdd去join
 *
 * 相关面试题：  spark如何实现map端join？（广播变量，闭包引用，缓存文件 [sc.addFile("hdfs://doit01:8020/abc/names.txt")]）
 */
object E02_RDD开发API边缘_广播变量 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("广播变量")


    val rdd: RDD[(Int, String)] = sc.parallelize(Seq(
      (1, "北京"),
      (2, "上海"),
      (3, "上海"))
    )

    // 普通对象 ，在Driver程序中创建的
    val mp = Map[Int, String]((1,"张三"),(2,"李四"),(3,"王五"))

    // 将driver端创建的普通集合对象，广播出去
    // 广播的实质是： 将driver端数据对象，序列化后，给每个executor发送一份（每个executor只持有一份广播变量的拷贝）
    // 广播变量的数据传输和闭包引用的数据传输有所不同：
    // 闭包引用的数据，是driver给每个executor直接发送数据
    // 广播变量的数据，是通过bittorrent协议来发送数据的（所有executor遵循了 人人为我，我为人人的原则）
    val bc = sc.broadcast(mp)

    val res = rdd.map(tp=>{
      val dict: Map[Int, String] = bc.value

      val name: String = dict.get(tp._1).get

      (tp._1,tp._2,name)

    })
    res.foreach(println)

    sc.stop()
  }

}


