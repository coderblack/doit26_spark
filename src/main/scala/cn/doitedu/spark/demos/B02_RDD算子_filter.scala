package cn.doitedu.spark.demos

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Person(id:Int,name:String,age:Int)


object RDD算子_filter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("集合映射RDD")

    val sc = new SparkContext(conf)

    /**
     * 1.加载文件成RDD  filter/input/x.txt  ，留下id为偶数的
     */
    // 加载文件
    val rdd: RDD[String] = sc.textFile("data/filter/input/x.txt")

    // 把 “1,a” 变成  (1,"1,a")
    val rdd2 =  rdd.map(s=>{
      val arr: Array[String] = s.split(",")
      (arr(0).toInt,s)
    })

    // 对上面的rdd按照id的奇偶属性过滤（留下偶数）
    val rdd3: RDD[(Int, String)] = rdd2.filter(tp => tp._1 % 2 == 0)


    /**
     * 2.加载文件成RDD  battel/input/a.txt  ，过滤掉数据中存在空字段的行
     */
    val rddBattel: RDD[String] = sc.textFile("data/battel/input/a.txt")
    val rddArray = rddBattel.map(s=>{
      val arr: Array[String] = s.split(",")
      arr
    })

    val res: RDD[Array[String]] = rddArray.filter(arr=>{
      arr.filter(StringUtils.isNotBlank(_)).size == 5
    })
    res.foreach(arr=>println(arr.mkString(",")))

    /**
     * 3.加载文件成RDD  json/input/a.txt  ，解析json数据成为Person对象数据，过滤掉json解析失败的行
     */
    val jsonRDD: RDD[String] = sc.textFile("data/json/input/")


    val personRDD: RDD[Person] = jsonRDD.map(s=>{

      try {
        val obj: JSONObject = JSON.parseObject(s)

        val id: Int = obj.getIntValue("id")
        val name: String = obj.getString("name")
        val age: Int = obj.getIntValue("age")

        Person(id, name, age)
      }catch{
        case e:Exception => null
      }

    })

    // 过滤
    val filtered: RDD[Person] = personRDD.filter(p => p != null)
    filtered.foreach(println)


    /**
     *   加载wordcount测试文件成为一个rdd
     *   然后对rdd中的字符串做切割，得到数组rdd
     *
     *   对数组rdd进行过滤：
     *     1. 数组元素个数<8的不要
     *     2. 数组中第一个元素以h开头的不要
     *     3. 数组中存在某个元素长度>6的不要
     *     可以在一个filter算子中实现，也可以分成3个filter算子来实现
     */
    val wcRDD: RDD[String] = sc.textFile("data/wordcount/input")
    val arrRDD: RDD[Array[String]] = wcRDD.map(s => s.split("\\s+"))

    val res2 = arrRDD.filter(arr=>arr.size>=8)
      .filter(arr => !arr(0).startsWith("h"))
      .filter(arr=> !arr.exists(s=>s.length>6))

    res2.foreach(println)


    sc.stop()
  }

}
