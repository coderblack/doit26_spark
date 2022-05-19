package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 统计连续登陆的天数>=3的用户，以及发生的起止日期
 *
 * - 这个问题可以扩展到很多相似的问题：连续几个月充值会员、连续天数有商品卖出、连续打滴滴、连续逾期。
 *
 * - 测试数据：用户ID、登入日期
 *
 * guid01,2018-02-28
 * guid01,2018-03-01
 * guid01,2018-03-02
 * guid01,2018-03-04
 * guid01,2018-03-05
 * guid01,2018-03-06
 * guid01,2018-03-07
 * guid02,2018-03-01
 * guid02,2018-03-02
 * guid02,2018-03-03
 * guid02,2018-03-06
 *
 *
 * 期望得到的结果：
 * guid01,4,2018-03-04,2018-03-07
 * guid02,3,2018-03-01,2018-03-03
 *
 * sql写一遍：
 * with tmp as (
 * select
 * guid,
 * dt,
 * row_number() over() as rn
 * from t
 * )
 * select
 * guid,
 * min(dt) as start_dt,
 * max(dt) as end_dt,
 * count(1) as days
 * from tmp
 * group by guid,date_sub(dt,rn)
 *
 */
object Y05_综合练习_5_方式2 {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("连续行为识别")

    val rdd: RDD[String] = sc.textFile("data/exersize/tm5/input")

    // 对rdd运算（切分数据）
    val result: RDD[(String, String, String, Int)] = rdd
      .map(s => {
        val arr: Array[String] = s.split(",")
        // guid,dt
        (arr(0), arr(1))
      })
      .groupByKey()
      .flatMap(tp => {
        // 取出guid
        val guid: String = tp._1
        // 对相同guid的一组日期进行排序
        val sortedDates: List[String] = tp._2.toList.sorted
        // 将一组日期和它自己的索引号绑定
        val ziped: List[(String, Int)] = sortedDates.zipWithIndex
        // 日期，日期-索引号
        val dateAndDiff: List[(String, Date)] = ziped.map(tp => (tp._1, DateUtils.addDays(DateUtils.parseDate(tp._1, "yyyy-MM-dd"), -tp._2)))
        // 按相同差值分组
        val dateToTuples: Map[Date, List[(String, Date)]] = dateAndDiff.groupBy(_._2)
        // 对相同差值的组（连续区间）进行结果数据求取
        dateToTuples.map(tp => {
          // guid,    起始日期       结束日期          连续登陆天数
          (guid, tp._2.head._1, tp._2.reverse.head._1, tp._2.size)
        })

      })

    result.foreach(println)

    sc.stop()
  }
}
