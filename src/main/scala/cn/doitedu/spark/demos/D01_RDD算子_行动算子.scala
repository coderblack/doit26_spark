package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.sql.{Connection, DriverManager}


case class Person2(id:Int,age:Int,salary:Double)

object D01_RDD算子_行动算子 {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = SparkContextUtil.getSc("行动算子测试")

    val rdd1: RDD[String] = sc.parallelize(Seq("a a a a a b", "c c c a b c a c", "d a f g a c d f"))
    val rdd2: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7,8))
    val rdd3: RDD[Person2] = sc.parallelize(Seq(Person2(1,18,9888),Person2(2,28,6800),Person2(3,24,12000)))


    // reduce 将rdd中的所有数据聚合成一个值
    val res: Int = rdd2.reduce(_ + _)  // 36


    // collect 将rdd中的数据汇总到driver端
    // 本算子慎用（因为一个rdd中数据体量庞大，汇总到driver端很容易引起内存不够）
    val collect: Array[String] = rdd1.collect()


    // count 计算rdd中的数据的条数
    val cnt: Long = rdd1.count()    // 3


    // first 取rdd中的第一条数据
    val str: String = rdd1.first()
    val i: Int = rdd2.first()

    // take(n) 从rdd中取n条数据
    val strings: Array[String] = rdd1.take(2)

    // takeSample() 随机抽样，并返回样本数据
    val sample: Array[Int] = rdd2.takeSample(true, 2)

    implicit val ord:Ordering[Person2] = new Ordering[Person2] {
      override def compare(x: Person2, y: Person2): Int = x.age.compare(y.age)
    }

    // takeOrdered
    val strings1: Array[String] = rdd1.takeOrdered(5)
    val persons: Array[Person2] = rdd3.takeOrdered(2)

    // countByKey() 统计rdd中每个key的数据条数
    val rdd4: RDD[(String, Int)] = rdd1.flatMap(s => s.split("\\s+")).map(w => (w, 1))
    /*
      val wc: RDD[(String, Int)] = rdd4.reduceByKey(_ + _)  // transformation算子
      val tuples: Array[(String, Int)] = wc.collect()
     */
    val wordcount: collection.Map[String, Long] = rdd4.countByKey() // action算子

    // foreach 对rdd中的每条数据执行一个指定的动作（函数）
    rdd1.foreach(s=>{
      val conn: Connection = DriverManager.getConnection("", "root", "123456")
      val stmt = conn.prepareStatement("insert into t1 values(?)")
      stmt.setString(1,s)
      stmt.execute()
    })

    // foreachPartition 类似foreach，但是会一次性给你整个分区数据的迭代器
    rdd1.foreachPartition(iter=>{

      val conn: Connection = DriverManager.getConnection("", "root", "123456")
      val stmt = conn.prepareStatement("insert into t1 values(?)")

      iter.foreach(s=>{
        stmt.setString(1,s)
        stmt.execute()
      })

    })


    //saveAsTextFile 将rdd的数据以文本文件的格式保存到文件系统中
    rdd1.saveAsTextFile("data/saveastextfile/")
    rdd1.saveAsTextFile("hdfs://doit01:8020/saveastextfile/")



    sc.stop()

  }
}
