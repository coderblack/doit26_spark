package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object S08_DataFrame输出到HIVE表 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("输出到hive")
      .config("hive.metastore.uris","thrift://doit01:9083")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    // 读取hdfs中的原始文件：订单销售记录   data/exersize/tm1_input/
    val schema = StructType(Seq(
      StructField("order_id",DataTypes.StringType),
      StructField("category_id",DataTypes.StringType),
      StructField("amount",DataTypes.StringType)
    ))

    val df: DataFrame = spark.read.schema(schema).csv("data/exersize/tm1_input/")

    // 将上面的dataframe存入hive的表  -- table api方式
    // df.write.saveAsTable("t_order")  // 如果指定的目标表不存在，sparksql会帮忙自动建表

    // -- 纯sql方式
    df.createTempView("df")
    /*spark.sql(
      """
        |
        |insert into table t_order
        |select
        |  order_id,category_id,amount
        |from df
        |
        |""".stripMargin)*/


    // 分区表的输出
    // 在hdfs的 data/acc_log/下有各天的日志数据，需要加载成dataframe，然后存储到hive的分区表中

    val schema2 = StructType(Seq(
      StructField("device_id",DataTypes.StringType),
      StructField("ip",DataTypes.StringType),
      StructField("url",DataTypes.StringType)
    ))

    val df03: DataFrame = spark.read.schema(schema2).csv("data/acc_log/2021-12-03")
    val df04: DataFrame = spark.read.schema(schema2).csv("data/acc_log/2021-12-04")


    // -- tableapi 实现的是动态分区的写法
    df03.selectExpr("device_id","ip","url"," '2021-12-03' as dt")
      .write.partitionBy("dt").saveAsTable("t_url")

    // 如果目标表已存在，需要指定一个saveMode：追加？ 覆盖？
    df04.selectExpr("device_id","ip","url"," '2021-12-04' as dt")
      .write.mode(SaveMode.Append).partitionBy("dt").saveAsTable("t_url")


    // -- 纯sql方式写入hive的分区表
    // -- 目标表必须提前存在
    /*
    create table t_url2(
       device_id string,
       ip string,
       url string
    )
    partitioned by (dt string)
    stored as parquet
    tblproperties(
      "parquet.compress"="snappy"
    )
    */
    df03.createTempView("df03")
    df04.createTempView("df04")
    /**   1.静态分区写法  */
    spark.sql(
      """
        |
        |insert into table t_url2 partition(dt='2021-12-03')
        |select * from df03;
        |
        |""".stripMargin)
    spark.sql(
      """
        |insert into table t_url2 partition(dt='2021-12-04')
        |select * from df04
        |
        |""".stripMargin)

    /**   2.动态分区写法  */
    spark.sql(
      """
        |insert into table t_url3 partition(dt)
        |select device_id,ip,url,'2021-12-03' as dt  from df03
        |union all
        |select device_id,ip,url,'2021-12-04' as dt  from df04
        |
        |""".stripMargin)



    spark.close()

  }

}
