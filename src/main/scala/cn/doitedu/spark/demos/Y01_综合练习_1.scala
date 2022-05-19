package cn.doitedu.spark.demos

import cn.doitedu.spark.util.SparkContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{JdbcRDD, RDD}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable

/**
 * 需求：根据IP规则文件，计算IP地址对应的归属地

ip规则数据: ip.txt （注意，规则文件中的数据是按照ip地址对应的十进制升序排序的）
1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302

说明：数据用|分隔，第一个字段是起始IP地址，第二个是结束IP地址，第三个是起始IP地址对应的十进制，第四个是结束IP地址对应的十进制，其余的是洲、国家、省份、市、区、运营商等


用户行为数据格式：ipaccess.log
20090121000132124542000|117.101.215.133|www.jiayuan.com|/19245971|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; TencentTraveler 4.0)|http://photo.jiayuan.com/

说明：数据用|分隔，第一个字段是时间、第二个是IP地址、第三个是域名、第四个是访问的地址

 */
object Y01_综合练习_1 {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextUtil.getSc("订单分析")

    // 加载文件
    val rdd: RDD[String] = sc.textFile("data/exersize/tm1_input")

    /**
     * 需求1：统计每个类目下的订单总金额
     */
    val res1 = rdd
      .map(s=>{
        val arr: Array[String] = s.split(",")
        (arr(1),arr(2).toDouble)
      })
      .reduceByKey(_+_)
      .sortBy(tp=>tp._2,false)

    // res1.saveAsTextFile("data/exersize/tm1_output1")


    /**
     * 需求2：关联维表（字典表），对原始数据补充类别名称字段
     */

    /**
     * 方式1
     * 在map算子处理每一条原始数据的过程中，去请求mysql查询类别ID对应的类别名称
     */
    val res2 = rdd.mapPartitions(iter=>{

      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val stmt: PreparedStatement = conn.prepareStatement("select name from tb_category where id=?")

      iter.map(line=>{
        val categoryId: String = line.split(",")(1)
        // 去mysql中查询 category Id 对应的 category Name
        stmt.setString(1,categoryId)
        val rs: ResultSet = stmt.executeQuery()
        rs.next()
        val categoryName = rs.getString("name")

        line + "," + categoryName
      })
    })

    // res2.saveAsTextFile("data/exersize/tm1_output2")


    /**
     * 方式2
     * 将mysql中的字典表加载为一个rdd，和订单rdd进行join
     */
    val conn = () => DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
    val sql = "select id,name from tb_category where keyid>=? and keyid<=?"
    val mapRow = (rs:ResultSet)=>{
      val categoryName = rs.getString("name")
      val id = rs.getString("id")
      (id,categoryName)
    }

    // 维表RDD，来自于mysql数据表
    val dictRdd = new JdbcRDD[(String, String)](sc, conn, sql, 1, 3, 1, mapRow)

    // 订单RDD，来自于订单记录文件
    val orderRdd = rdd.map(s=>{
      val arr: Array[String] = s.split(",")
      (arr(1),s)
    })

    // join
    val joined: RDD[(String, (String, Option[String]))] = orderRdd.leftOuterJoin(dictRdd)
    val res3 = joined.map(tp=>{
      tp._2._1+","+ (if(tp._2._2.isDefined) tp._2._2.get else "未知")
    })
    //res3.saveAsTextFile("data/exersize/tm1_output3")


    /**
     * 方式3
     * 3.1 先在driver端连接mysql，读取到维表数据，然后将维表数据作为广播变量发给各个executor来实现map端join
     */
    // 在driver端用jdbc连接请求mysql，读取整个维表，并保存到一个hashmap中
    val conn2: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
    val stmt: Statement = conn2.createStatement()
    val rs: ResultSet = stmt.executeQuery("select id,name from tb_category ")
    val dictMap = new mutable.HashMap[String, String]()
    while(rs.next()){
      val id: String = rs.getString("id")
      val name: String = rs.getString("name")
      dictMap.put(id,name)
    }
    // 将hashmap维表数据对象，广播出去
    val bc = sc.broadcast(dictMap)
    // 开始分布式处理订单数据
    val res4 = rdd.map(line=>{
      val categoryId: String = line.split(",")(1)
      val dict: mutable.HashMap[String, String] = bc.value
      val categoryName: String = dict.getOrElse(categoryId, "未知")
      line + "," + categoryName
    })
    res4.saveAsTextFile("data/exersize/tm1_output4")



    /**
     * 方式3
     * 3.2 将mysql中的维表加载为一个RDD，然后将RDD数据通过collect收集到driver端的hashmap中，然后将维表数据作为广播变量发给各个executor来实现map端join
     * 下面这个获取维表的方法，在本场景中不建议
     * 但不代表这种写法没有用武之地！！！
     */
    val dictMap2: Map[String, String] = dictRdd.collect().toMap



    sc.stop()
  }

}
