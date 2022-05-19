package cn.doitedu.spark.demos

import cn.doitedu.spark.util.{ExersizeUtils, SparkContextUtil}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * 需求：根据店铺的订单成交记录，计算月累计销售金额

数据格式：shop.csv

店铺金额,日期,订单金额
shop1,2019-01-18,500
shop1,2019-02-10,500
shop1,2019-02-10,200
shop1,2019-02-11,600
shop1,2019-02-12,400
shop1,2019-02-13,200
shop1,2019-02-15,100
shop1,2019-03-05,180
shop1,2019-04-05,280
shop1,2019-04-06,220
shop2,2019-02-10,100
shop2,2019-02-11,100
shop2,2019-02-13,100
shop2,2019-03-15,100
shop2,2019-03-16,300
shop2,2019-04-15,100
shop2,2019-04-20,200

期望结果：
店铺id,月份,当月销售金额,累计到当前月金额
shop2,2019-02,300,300
shop2,2019-03,400,700
shop2,2019-04,300,1000

 用sql写：
with tmp as (
   select
     shopid,
     month(dt) as mth,
     sum(amt) as mth_amt
   from t
   group by shopid,month(dt)
)

shop2,2019-02,300   -- 300          累计值：是从相同店铺数据的最前面一行累加到当前行
shop2,2019-03,400   -- 300+400      累计值：是从相同店铺数据的最前面一行累加到当前行
shop2,2019-04,300   -- 300+400+300  累计值：是从相同店铺数据的最前面一行累加到当前行

SELECT
  shopid,
  mth,
  mth_amt,
  sum(mth_amt) over(PARTITION BY shopid ORDER BY mth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as mth_accu_amt
FROM tmp

 */
object Y03_综合练习_3 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("月累计金额计算")

    // 读数据
    val groupedRDD: RDD[(String, Iterable[Array[String]])] = sc.textFile("data/exersize/tm3/input")
      .map(_.split(","))
      .groupBy(arr=>arr(0))


    // map统计上面rdd中的每一组
    val resRDD: RDD[(String, String, Int, Int)] = groupedRDD.flatMap(tp=>{
      // 店铺id
      val shopId: String = tp._1
      // 该店铺的所有销售记录
      val saleRecords: List[Array[String]] = tp._2.toList

      // 统计该店铺每月的销售总额，以及累计到该月的累计总额
      // [2019-04,List([shop1,2019-04-05,280],[shop1,2019-04-06,220])]
      val monthGroup: Map[String, List[Array[String]]] = saleRecords.groupBy(arr => arr(1).substring(0, 7))

      // List[(2019-01,200),(2019-03,400),(2019-02,300)]
      // 各月的总额
      val monthAmount: List[(String, Int,Int)] = monthGroup
        .map(tp => (tp._1,  tp._2.map(arr=>arr(2).toInt).sum , 0))
        .toList
        .sortBy(_._1)

      // 累计到各月的累计总额
      //  List[   (2019-01,200,0),   ,(2019-02,300,0),    (2019-03,400,0)]   =>
      //  ("",0,0)[(2019-01,200,200),(2019-02,300,500),(2019-03,400,900)]
      monthAmount
        .scan(("",0,0))((tp1,tp2)=>{(tp2._1,tp2._2,tp2._2+tp1._3)})
        .tail
        .map(ele=>(shopId,ele._1,ele._2,ele._3))
    })

    resRDD.foreach(println)


    sc.stop()
  }

}
