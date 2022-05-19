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
 *   guid,
 *   dt,
 *   row_number() over() as rn
 * from t
 * )
 * select
 *   guid,
 *   min(dt) as start_dt,
 *   max(dt) as end_dt,
 *   count(1) as days
 * from tmp
 * group by guid,date_sub(dt,rn)
 *
 */
object Y05_综合练习_5 {
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
        val guid: String = tp._1

        //         [2018-02-28,2018-03-01,2018-03-02,2018-03-05,2018-03-08,2018-03-09,2018-03-10,2018-03-13]

        // segment1[2018-02-28,2018-03-01,2018-03-02]
        // segments[segment1,]
        //                                  segment2[2018-03-05]
        // segments[segment1,segment2,]
        //                                              segment3[2018-03-08,2018-03-09,2018-03-10]
        // segments[segment1,segment2,segment3]
        //                                                                                   segment4[2018-03-13]

        val dtLst: List[String] = tp._2.toList.sorted

        val segments = new ListBuffer[List[Date]]()

        var segment = new ListBuffer[Date]()
        for (i <- 0 to dtLst.size - 1) {

          val dateI: Date = DateUtils.parseDate(dtLst(i), "yyyy-MM-dd")

          if (i != dtLst.size - 1 && DateUtils.isSameDay(DateUtils.addDays(dateI, 1), DateUtils.parseDate(dtLst(i + 1),"yyyy-MM-dd"))  ) {
            segment += dateI
          } else {
            segment += dateI
            segments += segment.toList

            segment = new ListBuffer[Date]()
          }
        }

        // List[(guid01,09-01,09-05,5),(09-08,09-13,6)]
        segments.filter(lst=>lst.size>=3).map(lst => {
          (guid, DateFormatUtils.format(lst.head, "yyyy-MM-dd"), DateFormatUtils.format(lst.reverse.head, "yyyy-MM-dd"), lst.size)
        })

      })


    result.foreach(println)


    sc.stop()
  }
}
