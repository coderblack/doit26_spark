package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD


case class Order(id:Int,uid:Int,amount:Double)
object D02_RDD算子_分区调整 {

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("分区调整")

    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 9, 10), 4)
    val rddkv1: RDD[(String, Int)] = sc.parallelize(Seq(("a",1),("b",2),("c",3)),2)
    val rddkv2: RDD[(Order,Int)] = sc.parallelize(Seq(
      (Order(1,10,100),1),
      (Order(2,10,80),1),
      (Order(3,12,100),1),
      (Order(2,10,200),1),
      (Order(3,12,180),1)
    ),2)


    // coalesce
    val rdd1 = rdd.coalesce(2,false)
    val rdd2 = rdd.coalesce(6,true)  // 调大必须设置shuffle=true

    println(rdd1.partitions.size)  // 2
    println(rdd2.partitions.size)  // 6


    // repartition  底层调用的就是 coalesce(?,true)
    val rdd3 = rdd.repartition(2)
    val rdd4 = rdd.repartition(10)

    println(rdd3.partitions.size)  // 2
    println(rdd4.partitions.size)  // 10


    // partitionBy  可以传入用户定义的Partitioner
    rddkv1.partitionBy(new HashPartitioner(3))  // HashPartitioner是将key的hashcode%分区数 来决定一条数据该分到哪个区

    // 如果key是一个复杂对象，而且要按对象中的某个属性来分区，则需要自己写分区器逻辑

    // 匿名内部实现类的写法
    rddkv2.partitionBy(new HashPartitioner(2){
      override def numPartitions: Int = super.numPartitions

      override def getPartition(key: Any): Int = {
        val od = key.asInstanceOf[Order]
        super.getPartition(od.id)
      }
    })

    // 普通写法（先定义好一个分区器类）
    rddkv2.partitionBy(new OrderPartition(2))


  }

}

class OrderPartition(partitions:Int) extends HashPartitioner(partitions:Int){
  override def numPartitions: Int = super.numPartitions

  override def getPartition(key: Any): Int = {
    val od = key.asInstanceOf[Order]
    super.getPartition(od.id)
  }
}


