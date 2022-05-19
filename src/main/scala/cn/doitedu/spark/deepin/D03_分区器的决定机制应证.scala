package cn.doitedu.spark.deepin

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

case class Stu(id: Int, name: String, gender: String, score: Double)

class StuGenderPartitioner(partitions:Int) extends HashPartitioner(partitions) {
  override def getPartition(key: Any): Int = {
    super.getPartition(key.asInstanceOf[Stu].gender)
  }
}

object D03_分区器的决定机制应证 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("不能太随便")
    conf.set("spark.default.parallelism", "4")

    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.textFile("data/wordcount/input")
    println(rdd.partitioner) // None

    val rdd2: RDD[(String, Int)] = rdd.flatMap(s => s.split("\\s+")).map((_, 1))
    println(rdd2.partitioner) // None

    val rdd31: RDD[(String, Int)] = rdd2.reduceByKey(new HashPartitioner(3),_ + _)
    // shuffle所产生的的rdd通常都有分区器 ，而且默认分区器都是HashPartitioner
    val rdd32: RDD[(String, Int)] = rdd2.reduceByKey(_ + _,3)  // reduceByKey(new HashPartitioner(numPartitions), func)

    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)  // reduceByKey(defaultPartitioner(self), func)
    println("没传任何分区相关参数的reduceByKey结果：" +  rdd3.partitioner) // HashPartitioner
    println("没传任何分区相关参数的reduceByKey结果：" +  rdd3.partitions.size) // HashPartitioner



    val rddStu = sc.parallelize(Seq(
      Stu(1, "a", "m", 94),
      Stu(2, "b", "f", 98),
      Stu(3, "c", "m", 92),
      Stu(4, "d", "f", 96),
      Stu(5, "e", "f", 98)
    ))


    // 在需要partitioner的算子中，不传分区器，则算子底层会生成一个默认分区器
    val rddGrouped1 = rddStu.groupBy(stu => stu.gender)
    println(rddGrouped1.partitioner) //  HashPartitioner

    val rddGrouped2 = rddStu.groupBy(stu => stu, new StuGenderPartitioner(4))
    println(rddGrouped2.partitioner) //  StuGenderPartitioner

    val rddSorted1: RDD[Stu] = rddStu.sortBy(stu => stu.score)
    println(rddSorted1.partitioner)  // None  在算子的内部中间过程中弄丢了

    val rddSorted2: RDD[(Double, Stu)] = rddStu.map(stu => (stu.score, stu)).sortByKey()
    println(rddSorted2.partitioner)  // Some(org.apache.spark.RangePartitioner@db961c50)



    sc.stop()
  }

}
