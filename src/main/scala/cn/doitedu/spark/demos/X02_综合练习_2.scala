package cn.doitedu.spark.demos

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 有如下学生信息，根据给定的经验样本数据 data/stu/input/sample.txt
 * 来对未知性别的数据(/data/stu/input/test.txt)进行性别预测
 */
object X02_综合练习_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("knn算法")
    val sc = new SparkContext(conf)

    // 加载样本数据集
    val sampleData: RDD[String] = sc.textFile("data/stu/input/sample.txt")

    // 数据加工
    val sample = sampleData.map(line=>{
      val arr: Array[String] = line.split(",")
      //(id,性别,属性数组)
      (arr(0),arr(4),List(arr(1).toDouble,arr(2).toDouble,arr(3).toDouble))
    })

    // 加载测试数据集
    val testData: RDD[String] = sc.textFile("data/stu/input/test.txt")
    val test = testData.map(line=>{
      val arr: Array[String] = line.split(",")
      //(id , 属性数组)
      (arr(0),List(arr(1).toDouble,arr(2).toDouble,arr(3).toDouble))
    })


    // 将两份数据做笛卡尔积
    val tmp: RDD[((String, List[Double]),(String, String, List[Double]))] = test.cartesian(sample)
    //tmp.foreach(println)
    /**
     *  ((x,List(176.0, 86.0, 79.0)),(1,m,List(170.0, 75.0, 87.0)))
        ((x,List(176.0, 86.0, 79.0)),(2,m,List(175.0, 80.0, 82.0)))
        ((x,List(176.0, 86.0, 79.0)),(3,m,List(168.0, 72.0, 77.0)))
        ((x,List(176.0, 86.0, 79.0)),(4,f,List(165.0, 55.0, 97.0)))
        ((x,List(176.0, 86.0, 79.0)),(5,f,List(160.0, 52.0, 95.0)))
        ((x,List(176.0, 86.0, 79.0)),(6,f,List(162.0, 48.0, 98.0)))
        ((y,List(162.0, 48.0, 96.0)),(1,m,List(170.0, 75.0, 87.0)))
        ((y,List(162.0, 48.0, 96.0)),(2,m,List(175.0, 80.0, 82.0)))
        ((y,List(162.0, 48.0, 96.0)),(3,m,List(168.0, 72.0, 77.0)))
        ((y,List(162.0, 48.0, 96.0)),(4,f,List(165.0, 55.0, 97.0)))
        ((y,List(162.0, 48.0, 96.0)),(5,f,List(160.0, 52.0, 95.0)))
        ((y,List(162.0, 48.0, 96.0)),(6,f,List(162.0, 48.0, 98.0)))
     */

    // 求未知性别的人，与每一个已知性别的样本人，求距离
    val tmp2 = tmp.map(tp=>{
      // 取出测试人员的属性数据
      val lst1: List[Double] = tp._1._2
      // 取出样本人员的属性数据
      val lst2: List[Double] = tp._2._3
      // 套公式：
      //val dist = Math.pow(lst1(0)-lst2(0),2.0) + Math.pow(lst1(1)-lst2(1),2.0) + Math.pow(lst1(2)-lst2(2),2.0)
      val dist = lst1.zip(lst2).map(tp=>Math.pow(tp._1-tp._2,2.0)).sum

      // 未知人id，样本性别，距离
      (tp._1._1,tp._2._2,dist)
    })
    //tmp2.foreach(println)
    /**
     * (x,m,221.0)
       (x,m,46.0)
       (x,m,264.0)
       (x,f,1406.0)
       (x,f,1668.0)
       (x,f,2001.0)
       (y,m,874.0)
       (y,m,1389.0)
       (y,m,973.0)
       (y,f,59.0)
       (y,f,21.0)
       (y,f,4.0)
     */

    // 从tmp2中取每个未知性别人距离最小的样本性别
    val grouped: RDD[(String, Iterable[(String, String, Double)])] = tmp2.groupBy(tp => tp._1)
    val result: RDD[(String, String, Double)] = grouped.map(tp => tp._2.minBy(tp => tp._3))

    result.foreach(println)




    sc.stop()
  }
}
