package cn.doitedu.spark.demos

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对 sanguo.txt和 heros.txt中的数据，做如下统计：
 *
 * 每个城市的杀人总数，关押总数
 * 每个城市的杀人最多的将军信息
 */
object X01_综合练习_1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("三国杀")
    val sc = new SparkContext(conf)

    // 加载战斗记录数据
    val battelRecords: RDD[String] = sc.textFile("data/sanguo/input")
    val tableA = battelRecords.map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0).toInt, (arr(1), arr(2).toInt))
    })

    // 加载将军信息数据
    val general: RDD[String] = sc.textFile("data/sanguo/heros")
    val tableB = general.map(line => {
      val jsonObject: JSONObject = JSON.parseObject(line)
      (jsonObject.getIntValue("id"),
        (
          jsonObject.getString("name"),
          jsonObject.getIntValue("age"),
          jsonObject.getString("city")
        )
      )
    })


    // 两表join
    val joined: RDD[(Int, ((String, Int), (String, Int, String)))] = tableA.join(tableB)
    //joined.foreach(println)
    /**
     * (4,((kill,6),(张无忌,24,深圳)))
     * (4,((kill,5),(张无忌,24,深圳)))
     * (4,((kill,3),(张无忌,24,深圳)))
     * (1,((kill,1),(关羽,28,上海)))
     * (1,((guanya,2),(关羽,28,上海)))
     * (6,((kill,5),(乔峰,38,广州)))
     * (3,((kill,5),(赵云,22,广州)))
     * (3,((guanya,3),(赵云,22,广州)))
     * (5,((kill,2),(虚竹,23,深圳)))
     * (5,((kill,4),(虚竹,23,深圳)))
     * (2,((kill,2),(张飞,38,北京)))
     * (2,((guanya,3),(张飞,38,北京)))
     * (2,((kill,6),(张飞,38,北京)))
     */

    // 计算每个城市的将军击杀人数，关押总数
    val tableC = joined.map(tp => {
      (tp._1, tp._2._1._1, tp._2._1._2, tp._2._2._1, tp._2._2._2, tp._2._2._3)
    })

    /**
     * (4,kill,6,张无忌,24,深圳)
     * (4,kill,5,张无忌,24,深圳)
     * (4,kill,3,张无忌,24,深圳)
     * (5,kill,2,虚竹,23,深圳)
     */

    val grouped: RDD[(String, Iterable[(Int, String, Int, String, Int, String)])] = tableC.groupBy(tp => tp._6)
    /**
     * 深圳 -> Iterator [ (4,kill,6,张无忌,24,深圳) ,(4,kill,5,张无忌,24,深圳),(4,guanya,3,张无忌,24,深圳),(5,kill,2,虚竹,23,深圳)]
     * 上海 ->Iterator[    ]
     * 北京 ->Iterator[    ]
     * 广州 ->Iterator[    ]
     */

    // grouped在后续的多个需求中要共用
    // 可以通过cache方法，将其进行缓存（默认在内存里）
    grouped.cache()


    /**
     * 需求1： 求城市击杀总数和关押总数
     */
    val result = grouped.map(tp => {
      val city: String = tp._1
      val iter = tp._2
      // 求该城市的击杀总数
      val killAmount: Int = iter.filter(tp => tp._2.equals("kill")).map(tp => tp._3).sum
      // 求关押总数
      val guanyaAmount: Int = iter.filter(tp => tp._2.equals("guanya")).map(tp => tp._3).sum

      (city, killAmount, guanyaAmount)
    })
    result.foreach(println)


    println("-------------------分割线---------------------")
    /**
     * 需求2： 求每个城市击杀人数最多的将军
     */
    /**
     * (深圳 ,  Iterator [ (4,kill,6,张无忌,24,深圳) ,(4,kill,5,张无忌,24,深圳),(4,guanya,3,张无忌,24,深圳),(5,kill,2,虚竹,23,深圳)])
     * (上海 , Iterator[    ])
     * (北京 , Iterator[    ])
     * (广州 , Iterator[    ])
     */
    val maxKill = grouped.map(tp=>{
      val city: String = tp._1
      tp._2.filter(tp => tp._2.equals("kill")).maxBy(tp => tp._3)
    })
    maxKill.foreach(println)


    sc.stop()
  }
}
