package cn.doitedu.spark.sql_demos

import cn.doitedu.spark.util.BitMapUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object S14_SPARKSQL的UDAF自定义函数应用实战 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("自定义UDAF")
      .master("local")
      .config("spark.sql.shuffle.partitions",2)
      .enableHiveSupport()
      .getOrCreate()


    // 加载待处理数据
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("province",DataTypes.StringType),
      StructField("city",DataTypes.StringType),
      StructField("region",DataTypes.StringType),
      StructField("pv",DataTypes.IntegerType),
    ))
    val df: DataFrame = spark.read.schema(schema).csv("data/udaf/input")
    df.createTempView("df")

    // 注册自定义UDAF
    import org.apache.spark.sql.functions.udaf
    spark.udf.register("gen_bitmap",udaf(BitMapGenUDAF))
    spark.udf.register("merge_bitmap",udaf(BitMapOrMergeUDAF))

    val card = (bmBytes:Array[Byte])=>{
      BitMapUtil.deSerBitMap(bmBytes).getCardinality
    }
    spark.udf.register("card_bm",card)


    // 按省市区统计pv总数和uv总数并保存到hive中
    val pcrReport = spark.sql(
      """
        |
        |select
        |province,
        |city,
        |region,
        |sum(pv) as pv_amt,
        |card_bm(gen_bitmap(id)) as uv_cnt,
        |gen_bitmap(id) as bitmap
        |
        |from df
        |group by province,city,region
        |
        |
        |""".stripMargin)
    pcrReport.write.saveAsTable("pcr_report")


    // 读hive中省市区报表，聚合出省市报表
    spark.sql(
      """
        |
        |select
        |province,
        |city,
        |sum(pv_amt)  as pv_amt,
        |card_bm(merge_bitmap(bitmap)) as uv_cnt,
        |merge_bitmap(bitmap) as bitmap
        |
        |from pcr_report
        |group by province,city
        |
        |""".stripMargin).show(100,false)


    // 读hive中省市区报表，聚合出省报表
    spark.sql(
      """
        |
        |select
        |province,
        |sum(pv_amt)  as pv_amt,
        |card_bm(merge_bitmap(bitmap)) as uv_cnt,
        |merge_bitmap(bitmap) as bitmap
        |
        |from pcr_report
        |group by province
        |
        |""".stripMargin).show(100,false)


    spark.close()


  }

}
