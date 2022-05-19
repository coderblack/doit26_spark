package cn.doitedu.spark.sql_exercise

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 对店铺销售记录数据统计如下报表：
 *    shopid,月份,月销售总额,累积到该月的销售总额
 *
 */
object X01_销售统计 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    // shopid,dt,sale_amount
    val schema = StructType(Seq(
      StructField("shopid",DataTypes.StringType),
      StructField("dt",DataTypes.StringType),
      StructField("sale_amount",DataTypes.DoubleType)
    ))
    val df: DataFrame = spark.read.option("header", "true").schema(schema).csv("data/csv/input/c.csv")

    // 注册一个临时视图名
    df.createTempView("t")

    // 执行sql
    val df2 = spark.sql(
      """
        |
        |select
        |   shopid,
        |   substr(dt,0,7) as month,
        |   sum(sale_amount) as month_amount
        |   --sum(month_amount) over(partition by shopid order by substr(dt,0,7) rows between unbounded preceding and current row) as accumulate_amount
        |from t
        |group by shopid,substr(dt,0,7)
        |order by shopid,month_amount
        |
        |""".stripMargin)
    /**
      * +------+-------+------------+
       |shopid|month  |month_amount|
       +------+-------+------------+
       |s001  |2021-01|3000.0      |    ?
       |s001  |2021-02|3000.0      |    ?
       |s001  |2021-03|8000.0      |    ?
       |s001  |2021-04|7000.0      |
       |s001  |2021-05|1500.0      |   ?
       |s002  |2021-02|1000.0      |
       |s002  |2021-05|1500.0      |
       |s002  |2021-03|1500.0      |
       |s002  |2021-01|5000.0      |
       |s002  |2021-04|6500.0      |
       +------+-------+------------+
     */

    df2.createTempView("t2")
    spark.sql(
      """
        |
        |select
        |   shopid,
        |   month,
        |   month_amount,
        |   sum(month_amount) over(partition by shopid order by month rows between unbounded preceding and current row) as accumulate_amount
        |from t2
        |
        |""".stripMargin).show(100,false)



    spark.close()
  }


}
