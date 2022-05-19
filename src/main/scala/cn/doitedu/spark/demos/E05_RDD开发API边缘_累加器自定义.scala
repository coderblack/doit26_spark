package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-11-25
 * @desc 累加器，主要用于正常业务作业过程中的一些附属信息统计
 *       （比如程序遇到的脏数据条数，
 *       程序处理的数据总行数，
 *       程序处理的特定数据条数，
 *       程序处理所花费的时长）
 */
object E05_RDD开发API边缘_累加器自定义 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextUtil.getSc("累加器")

    val id_malformed: LongAccumulator = sc.longAccumulator("id_malformed")
    val fields_notenough: LongAccumulator = sc.longAccumulator("fields_notenough")

    // 构造一个自定义的累加器
    val accumulator = new CustomAccumulator(new mutable.HashMap[String, Long]())
    // 将自定义累加器向sparkContext注册
    sc.register(accumulator)

    val info: RDD[String] = sc.parallelize(
      Seq(
      "1,Mr.duan,18,beijing",
      "2,Mr.zhao,28,beijing",
      "b,Mr.liu,24,shanghai",
      "4,Mr.nai,22,shanghai",
      "a,Mr.liu,24,shanghai",
      "6,Mr.ma")
    )

    val tuples = info.map(line => {
      var res: (Int, String, Int, String) = null
      try {
        val arr: Array[String] = line.split(",")
        val id = arr(0).toInt
        val name = arr(1)
        val age = arr(2).toInt
        val city = arr(3)
        res = (id, name, age, city)
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          fields_notenough.add(1)
          accumulator.add(("indexoutofBounds",1))
        }
        case e: NumberFormatException => {
          id_malformed.add(1)
          accumulator.add(("numberFormatException",1))
        }
        case _ => accumulator.add(("other",1))
      }
      res
    })

    val res = tuples.filter(tp => tp != null)
      .groupBy(tp => tp._4)
      .map(tp => {
        (tp._1, tp._2.size)
      })

    res.foreach(println)

    // 查看累加器的数值
    println(id_malformed.value)
    println(fields_notenough.value)
    println(accumulator.value)


    sc.stop()
  }

}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-11-25
 * @desc IN泛型，指的是在累加器上调add()方法所传入的参数的类型
 *       OUT泛型，指的是在累加器上调value所返回的数据的类型
 *       注意：累加器中用于存储值的数据结构，必须是线程安全的；
 *       因为累加器对象在一个executor中是被多个task线程共享；
 *       本例中的HashMap其实是不合适的！！！！！
 *       应该改成 java.util.concurrent.ConcurrentHashMap
 */
class CustomAccumulator(valueMap: scala.collection.mutable.HashMap[String, Long]) extends AccumulatorV2[(String, Int), scala.collection.mutable.HashMap[String, Long]] {

  // 判断累加器是否为初始状态
  override def isZero: Boolean = valueMap.isEmpty

  // 拷贝一个累加器对象的方法
  override def copy(): AccumulatorV2[(String, Int), scala.collection.mutable.HashMap[String, Long]] = {

    val newValueMap = new mutable.HashMap[String, Long]()

    this.valueMap.foreach(kv=>newValueMap.put(kv._1,kv._2))

    new CustomAccumulator(newValueMap)
  }

  // 将累加器重置的方法
  override def reset(): Unit = this.valueMap.clear()

  // 对累加器更新值的方法
  override def add(tp: (String, Int)): Unit = {
    val key: String = tp._1
    val oldValue: Long = this.valueMap.getOrElse(tp._1, 0)

    this.valueMap.put(key, oldValue+tp._2)
  }

  // driver端对各个task所产生的的累加器进行结果合并的逻辑
  override def merge(other: AccumulatorV2[(String, Int), mutable.HashMap[String, Long]]): Unit = {
    val otherValueMap: mutable.HashMap[String, Long] = other.value
    otherValueMap.foreach(kv => {
      val key: String = kv._1
      val selfValue: Long = this.valueMap.getOrElse(kv._1, 0)
      val otherValue: Long = otherValueMap(kv._1)

      this.valueMap.put(key,  selfValue+otherValue )
    })
  }

  // 从累加器上取值
  override def value: scala.collection.mutable.HashMap[String, Long] = this.valueMap
}
