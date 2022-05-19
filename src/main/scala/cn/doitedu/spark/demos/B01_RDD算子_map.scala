package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Student(id:Int,name:String,gender:String,age:Int,city:String)
case class Teacher(id:Int,name:String,gender:String,course:String)

object RDD算子_map {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("集合映射RDD")

    val sc = new SparkContext(conf)

    // 造一个list集合，装着一些单词，映射成RDD，然后对RDD进行转换，将单词全部变成大写
    val lst = List(
      "abc","bbc","fbi","nbc","lol"
    )
    val rdd: RDD[String] = sc.parallelize(lst)

    val rdd2: RDD[String] = rdd.map(s => s.toUpperCase)
    rdd2.foreach(println)


    // 将上面的rdd2转成  （单词,1） 的rdd
    val rdd3: RDD[(String, Int)] = rdd2.map(s => (s, 1))
    rdd3.foreach(println)

    // 将上面的rdd转成 (单词,n） , 以a开头的单词，n为1， 以b开头的单词，n为2，其他为3
    val rdd4: RDD[(String, Int)] = rdd2.map(s => {
      if (s.startsWith("A")) (s, 1) else if (s.startsWith("B")) (s, 2) else (s, 3)
    })

    // 造一个字符串集合，映射成RDD,字符串内容类似于: 1,zhangsan,male,28,shanghai
    val persons = List(
      "1,zhangsan,female,28,shanghai",
      "2,lisi,male,18,shanghai",
      "3,wangwu,female,38,beijing",
      "4,zhaoliu,male,26,shanghai"
    )
    val rdd5: RDD[String] = sc.parallelize(persons)
    // 将上面的rdd5转换成 RDD[Student]
    val rdd6: RDD[Student] = rdd5.map(s=>{
      val arr: Array[String] = s.split(",")
      Student(arr(0).toInt,arr(1),arr(2),arr(3).toInt,arr(4))
    })

    // 造一个数组集合，映射成RDD，数组类似： [1,xingge,male,flink]
    val seq: Seq[Array[String]] = Seq(
      Array("1","xingge","male","flink"),
      Array("2","laohu","male","javase"),
      Array("3","naige","female","javase"),
      Array("4","hangge","male","hadoop"),
    )
    val rdd7: RDD[Array[String]] = sc.parallelize(seq)
    // 将这个rdd 转成 RDD[Teacher]
    val rdd8: RDD[Teacher] = rdd7.map(arr=>{
      Teacher(arr(0).toInt,arr(1),arr(2),arr(3))
    })

    sc.stop()

  }

}
