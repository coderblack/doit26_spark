package cn.doitedu.spark.demos

import cn.doitedu.spark.util.{ExersizeUtils, SparkContextUtil}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator


/**
 *
需求：根据IP规则文件，计算IP地址对应的归属地

ip规则数据: ip.txt （注意，规则文件中的数据是按照ip地址对应的十进制升序排序的）
1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302

说明：数据用|分隔，第一个字段是起始IP地址，第二个是结束IP地址，第三个是起始IP地址对应的十进制，第四个是结束IP地址对应的十进制，其余的是洲、国家、省份、市、区、运营商等

用户行为数据格式：ipaccess.log
20090121000132124542000|117.101.215.133|www.jiayuan.com|/19245971|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; TencentTraveler 4.0)|http://photo.jiayuan.com/

说明：数据用|分隔，第一个字段是时间、第二个是IP地址、第三个是域名、第四个是访问的地址
 */

case class IpRule(startIp:Long,endIp:Long,zhou:String,country:String,province:String,city:String,district:String)
object X04_综合练习_4 {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = SparkContextUtil.getSc("IP地址归属地查询")
    val dirtyCnt: LongAccumulator = sc.longAccumulator("dirty_cnt")

    // 加载ip规则数据
    val ipRuleRdd: RDD[String] = sc.textFile("data/exersize/tm2/iprule")
    val ipAreaInfoRdd = ipRuleRdd.map(s=>{
      val arr: Array[String] = s.split("\\|")
      IpRule(arr(2).toLong,arr(3).toLong,arr(4),arr(5),arr(6),arr(7),arr(8))
    })
    // 将ip地理位置信息数据广播出去
    val rules: Array[IpRule] = ipAreaInfoRdd.collect().sortBy(iprule=>iprule.startIp)

    val bc: Broadcast[Array[IpRule]] = sc.broadcast(rules)

    // 加载用户访问日志
    val rdd: RDD[String] = sc.textFile("data/exersize/tm2/logdata")
    // 抽取日志数据中的ip地址
    val ipRdd = rdd.map(s => {
      var rt:(String,String) = null
      try {
        val arr: Array[String] = s.split("\\|")
        rt = (arr(1), s)
      } catch {
        case e: Exception => dirtyCnt.add(1);
        case _: Exception => dirtyCnt.add(1);
      }
      rt
    })
    // 脏数据过滤
    val filteredRdd: RDD[(String, String)] = ipRdd.filter(_ != null)

    // 查询ip的归属地，核心代码
    val result = filteredRdd.mapPartitions(iter=>{

      // 13783487436,1767683400,亚洲,中国,上海市,上海市,闵行区
      // 29375843888,2980000000,亚洲,中国,上海市,上海市,长宁区
      val ipAreaDict: Array[IpRule] = bc.value  // 取出广播变量


      // 对分区中的数据进行逐条迭代处理（通过ip查询归属地）
      iter.map(tp=>{
        // 取出日志数据中的ip地址
        val ip: String = tp._1

        // 对分段字符串形式的IP地址：20.0.1.0 , 按照.号切割
        val ipSegs: Array[Long] = ip.split("\\.").map(_.toLong)
        // 将分段字符串形式的IP地址转成10进制整数，如： 14583989434
        val ipInt: Long = ipSegs(0)*256*256*256 + ipSegs(1)*256*256 + ipSegs(2)*256 + ipSegs(3)

        // 将查询到的地理名称字段拼接到原来的日志数据后面，返回
        var Array(zhou:String,country:String,province:String,city:String,district:String) =Array("","","","","")
        val idx: Int = ExersizeUtils.binarySearch(ipAreaDict, ipInt)
        if(idx != -1){
          val rule: IpRule = ipAreaDict(idx)
          zhou = rule.zhou
          country = rule.country
          province = rule.province
          city = rule.city
          district = rule.district
        }
        s"${tp._2}|${zhou}|${country}|${province}|${city}|${district}"
      })
    })


    // 保存结果
    result.saveAsTextFile("data/exersize/tm2/output")

    sc.stop()
  }

}
