package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 流量统计，将同一个用户的多个上网行为数据进行聚合
 * 如果两次上网相邻时间小于10分钟，就累计到一起
 *
 */
case class Action(uid:String,startTime:String,endTime:String,num:Int)

object Y06_综合练习_6 {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("行为间隔分段统计")

    val rdd: RDD[String] = sc.textFile("data/exersize/tm6/input")

    val beanRDD = rdd.map(line=>{
      val arr: Array[String] = line.split(",")
      Action(arr(0),arr(1),arr(2),arr(3).toInt)
    })


    val inInterval = (dt1:String,dt2:String)=>{
      val d1: Date = DateUtils.parseDate(dt1, "yyyy-MM-dd HH:mm:ss")
      val d2: Date = DateUtils.parseDate(dt2, "yyyy-MM-dd HH:mm:ss")
      d2.getTime - d1.getTime <= 10*60*1000
    }

    // 对rdd分组
    val result: RDD[(String, String, String, Int)] = beanRDD.groupBy(bean=>bean.uid)
      .flatMap(tp=>{
        val uid: String = tp._1
        val actions: List[Action] = tp._2.toList.sortBy(bean=>bean.startTime)

        val segments = new ListBuffer[List[Action]]()
        var segment = new ListBuffer[Action]()
        // 划分区间段
        for(i <- 0 to actions.size-1){
          if( i != actions.size-1 && inInterval(actions(i).endTime,actions(i+1).startTime) ){
            segment += actions(i)
          }else{
            segment += actions(i)
            segments += segment.toList
            segment = new ListBuffer[Action]()
          }
        }

        // 取结果
        segments.map(lst=>{
          // 段落的起始时间,段落的结束时间,段落的num累计和
          (uid,lst.head.startTime,lst.reverse.head.endTime,lst.map(bean=>bean.num).sum)
        })
      })

    result.foreach(println)

    sc.stop()
  }
}
