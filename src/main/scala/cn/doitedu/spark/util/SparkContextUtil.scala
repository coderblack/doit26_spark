package cn.doitedu.spark.util

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextUtil {


  def getSc(appName:String,master:String="local"):SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)

    new SparkContext(conf)

  }

}
