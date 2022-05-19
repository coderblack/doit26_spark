package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-11-25
 * @desc spark中这个所谓“闭包”，只是看起来类似各种编程语言中的闭包
 * 而本质上根本不是一回事；
 * spark中的这个“闭包引用”,其实是Driver把分布式算子中引用的外部变量序列化后，发送给每个task来使用
 * 闭包引用的目标对象必须是可序列化的！！！而且数据量不能太大
 */
object E01_RDD开发API边缘_闭包 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("闭包问题")
    val rdd: RDD[(Int, String)] = sc.parallelize(Seq((1, "北京"), (2, "上海")))

    // 普通对象 ，在Driver程序中创建的
    val mp = new mutable.HashMap[Int, String]()
    mp.put(1,"张三")
    mp.put(2,"李四")

    // object对象 ，在Driver程序中创建的
    val data = JobName


    // rdd的计算在集群中task程序中运行
    val rdd2 = rdd.map(tp=>{
      // 分布式算子中函数，如果引用了外部的变量，则driver会把该变量序列化后通过网络发送给每一个task
      // 普通对象在每个task线程中都持有一份，不存在任何线程安全问题
      val name: String = mp.get(tp._1).get

      // 单例对象只在每个executor中持有一份，由executor中的多个task线程共享
      // 不要在这里对该变量进行任何修改操作，否则会产生线程安全问题
      val jobName: String = data.job

      // 闭包函数内，对外部变量做了修改
      mp.put(1,"张33333")
      println(mp) // Map(2 -> 李四, 1 -> "张33333333")

      (tp._1,tp._2,name,jobName)
    })

    rdd2.foreach(println)


    // 闭包外这个变量是被真正修改了吗？
    // driver端的这个对象是没有发生任何改变的，因为算子里面的修改动作是在task中执行，task中对自己持有的“拷贝”做了修改
    println(mp)  // 1,张三


    sc.stop()
  }

}


