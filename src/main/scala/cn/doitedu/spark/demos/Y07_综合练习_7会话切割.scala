package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.lang
import java.util.UUID

object Y07_综合练习_7会话切割 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("会话分割")


    // 加载数据源
    val rdd: RDD[String] = sc.textFile("data/applog/input/app_log_2021-06-05.log")

    // json解析
    val kvRdd = rdd.map(json=>{

      val obj: JSONObject = JSON.parseObject(json)

      val oldSessionId: String = obj.getString("sessionId")

      (oldSessionId,obj)
    })

    // 按原始会话id对数据分组
    val grouped: RDD[(String, Iterable[JSONObject])] = kvRdd.groupByKey()


    // 对每一组数据进行处理
    val res: RDD[JSONObject] = grouped.flatMap(tp=>{

      // 原始会话id相同的一组行为事件
      val list: List[JSONObject] = tp._2.toList.sortBy(obj=>obj.getLong("timeStamp"))

      // 生成一个 统计用会话id
      var statisticSessionId: String = RandomStringUtils.randomAlphabetic(10).toUpperCase()

      for(i <- 0 until list.size) {

        list(i).put("statisticSessionId",statisticSessionId)

        if(i!=list.size-1 &&  (list(i+1).getLong("timeStamp") - list(i).getLong("timeStamp") > 10*60*1000))
          statisticSessionId = RandomStringUtils.randomAlphabetic(10).toUpperCase()
      }

      list
    })


    res.saveAsTextFile("data/applog/output")

    sc.stop()

  }

}
