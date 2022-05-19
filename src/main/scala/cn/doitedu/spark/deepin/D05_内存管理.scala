package cn.doitedu.spark.deepin

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util
import scala.collection.mutable.ListBuffer


case class Person(id:Int,name:String,age:Int)
object D05_内存管理 {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("")

    val rdd: RDD[String] = sc.textFile("path")

    val rdd2 = rdd.mapPartitions(iter=>{

      val lst = new ListBuffer[Person]

      while(iter.hasNext){
        val str: String = iter.next()
        val arr: Array[String] = str.split(",")
        lst += Person(arr(0).toInt,arr(1),arr(2).toInt)
      }
      lst.toIterator
    })
    rdd2.cache()

    rdd2.map(p=>(p.id,p)).reduceByKey((p1,p2)=>p1)

    val rdd22 = rdd.map(s=>{
      val arr: Array[String] = s.split(",")
      Person(arr(0).toInt,arr(1),arr(2).toInt)
    })
    rdd22.map(p=>(p.id,p)).reduceByKey((p1,p2)=>p1)


    rdd.mapPartitions(iter=>{

      iter.map(s=>{
        val arr: Array[String] = s.split(",")
        Person(arr(0).toInt,arr(1),arr(2).toInt)
      })
    })


  }

}
