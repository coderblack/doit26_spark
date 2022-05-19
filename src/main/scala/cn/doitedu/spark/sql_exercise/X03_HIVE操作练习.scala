package cn.doitedu.spark.sql_exercise

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-12-04
 * @desc 读取 data/exersize/tm3/input 下的店铺销售数据
 * 并将读到的数据，写入hive的表  t_shop_sales
 * 然后用sparksql读hive的表 t_shop_sales ，进行统计分析（计算每个店铺，每个月份的总销售额及累计到改月的累计金额）
 * 然后将统计结果数据写入hive的表
 */
object X03_HIVE操作练习 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("X03_HIVE操作练习")
      .config("hive.metastore.uris","thrift://doit01:9083")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("shop_id",DataTypes.StringType),
      StructField("dt",DataTypes.StringType),
      StructField("amount",DataTypes.DoubleType)
    ))
    val df: DataFrame = spark.read.schema(schema).csv("data/exersize/tm3/input")

    // 存入hive
    df.write.saveAsTable("t_shop_sales")


    // 读hive中的 t_shop_sales表 ,直接统计,输出结果
    val res: DataFrame = spark.sql(
      """
        |select
        |   shop_id,
        |   month(dt) as mth,
        |   sum(amount)  as amt,
        |   sum(sum(amount)) over(partition by shop_id order by month(dt) rows between unbounded preceding and current row) as acu_amt
        |
        |from t_shop_sales
        |group by shop_id,month(dt)
        |
        |""".stripMargin)

    res.write.saveAsTable("t_shop_stat")

    spark.close()
  }
}
