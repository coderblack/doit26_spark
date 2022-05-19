package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.SparkSession

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-12-04
 * @desc sparksql要访问hive
 * 需要如下要素：
 * 1. hive-site.xml  主要是指定hive的元数据服务所在地址
 * 2. pom.xml中，需要加入  spark-hive 这个整合jar包
 * 3. sparkSession还要开启hive支持开关
 */
object S07_DataFrame创建之HIVE表 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("hive访问演示")
      .master("local")
      .config("hive.metastore.uris","thrift://doit01:9083")  // 如果配置文件没有被成功加载，可以在代码中设置这个元数据服务地址参数
      .enableHiveSupport()
      .getOrCreate()


    val res1 = spark.sql("""select * from t_sparktest""")  // 纯sql
    val res2 = spark.read.table("t_sparktest")  // table api

    // res1.show(100,false)
    // res2.show(100,false)


    // 读取hive的分区表  并指定要读取的分区
    spark.sql(""" select * from t_acc_log where dt='2021-12-03'  """).show(100,false)
    spark.read.table("t_acc_log").where(" dt='2021-12-04' ").show(100,false)


    spark.close()

  }
}
