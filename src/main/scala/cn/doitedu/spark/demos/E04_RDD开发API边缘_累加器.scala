package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-11-25
 * @desc 累加器，主要用于正常业务作业过程中的一些附属信息统计
 * （比如程序遇到的脏数据条数，
 * 程序处理的数据总行数，
 * 程序处理的特定数据条数，
 * 程序处理所花费的时长）
 */
object E04_RDD开发API边缘_累加器 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("累加器")

    val jsonRDD: RDD[String] = sc.textFile("data/json/input/")

    // 用一个普通变量，想在task中进行累加更新，是行不通的
    var malformedJsonLines:Int = 0

    // 正确打开方式是： 创建一个spark框架所提供的“Accumulator”累加器
    val accumulator: LongAccumulator = sc.longAccumulator("malformedJsonLines")
    val calcTimeLongAccumulator: LongAccumulator = sc.longAccumulator("calcTimeLongAccumulator")

    // 案例需求：解析json，并附带统计出脏数据条数，以及解析函数所耗费的总时长
    val rdd2: RDD[(String, Int)] = jsonRDD.map(json => {
      val start: Long = System.currentTimeMillis()
      var res :(String,Int)= null

      try {
        val jsonObject: JSONObject = JSON.parseObject(json)
        val id: Integer = jsonObject.getInteger("id")
        val name: String = jsonObject.getString("name")
        val sex: String = jsonObject.getString("sex")
        val age: Integer = jsonObject.getInteger("age")
        res = (sex, age)
      } catch {
        case e: Exception => {
          // malformedJsonLines += 1  // 并不会修改掉driver端的累计变量
          accumulator.add(1)
        }
      }

      val end: Long = System.currentTimeMillis()
      calcTimeLongAccumulator.add(end-start)

      res
    })
    rdd2.collect()


    // println(malformedJsonLines)  // 并不会修改掉driver端的累计变量
    println(s"不合法的json行数为： ${accumulator.value}")
    println(s"json解析函数的运算总耗时： ${calcTimeLongAccumulator.value}")

    sc.stop()
  }

}
