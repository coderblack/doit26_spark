package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B11_RDD算子_aggregateByKey {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("mappartitions测试")

    val sc = new SparkContext(conf)


    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a",2),("a",3), ("b",1),("b",4),("c",1),("a",6),("b",6)), 2)

    // 需求1： 将相同key的元素进行累加
    val f11 = (u:Int,e:Int)=>{u+e}
    val f12 = (u1:Int,u2:Int)=>u1+u2

    //zeroValue 是做聚合是的初始值（初始值只在分区内聚合的时候使用；在分区间聚合的时候不用）
    // f1 用于分区内的局部聚合逻辑
    // f2 用于各分区局部聚合结果的全局聚合逻辑
    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(f11,f12)
    // rdd2.foreach(println)


    // 需求2： 将相同key的元素聚合成一个List
    val f21 = (u:List[Int],e:Int)=>{e::u}
    val f22 = (lst1:List[Int],lst2:List[Int])=>{lst1:::lst2}
    val rdd3  = rdd.aggregateByKey(List[Int]())(f21, f22)
    rdd3.foreach(println)


    /**
     * aggregate 不bykey
     * 针对那种非kv结构数据的聚合
     */

    val rddx: RDD[Int] = sc.parallelize(Seq(1, 1, 1, 1, 1, 2, 2, 2, 2, 2), 3)

    // aggregate已经是一个行动算子，因为它要返回一个具体的聚合值
    // 这个算子的初始值，在分区内局部聚合的时候以及分区间聚合的时候都会被使用到
    val res: Int = rddx.aggregate(100)((u, e) => u + e, (u1, u2) => u1 + u2)
    println(res)   // 415


    sc.stop()

  }

}
