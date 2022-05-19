package cn.doitedu.spark.util

import cn.doitedu.spark.demos.IpRule

object ExersizeUtils {

  /**
   * 二分搜索算法
   * @param ipAreaDict
   * @param ipInt
   * @return
   */
  def binarySearch(ipAreaDict: Array[IpRule],ipInt:Long):Int = {
    var low = 0;
    var high = ipAreaDict.size-1;

    while(low<=high){
      val mid = (low+high)/2
      if(ipCompare(ipInt,ipAreaDict(mid)) == 0){
        return mid
      }else if(ipCompare(ipInt,ipAreaDict(mid)) == -1){
        high = mid-1
      }else {
        low = mid +1
      }
    }
    return -1
  }

  def ipCompare(ipInt:Long,ipRule:IpRule):Int = {
    var res = 0
    if(ipInt<ipRule.startIp) res = -1
    if(ipInt>ipRule.endIp) res = 1
    res
  }


  def main(args: Array[String]): Unit = {

    val arr = Array(IpRule(10,100,"a","a","a","a","a"),IpRule(110,200,"b","b","b","b","b"),IpRule(220,350,"c","c","b","b","b"))

    val idx: Int = binarySearch(arr, 400)

    println(idx)



  }
}
