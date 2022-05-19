package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object S13_SPARKQL的UDF自定义函数应用实战 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("自定义函数demo")
      .master("local")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("f1",DataTypes.IntegerType),
      StructField("f2",DataTypes.IntegerType),
      StructField("f3",DataTypes.IntegerType),
      StructField("gender",DataTypes.StringType),
    ))
    val sample: DataFrame = spark.read.schema(schema).csv("data/stu/input/sample.txt")


    val schema2 = StructType(Seq(
      StructField("id",DataTypes.StringType),
      StructField("f1",DataTypes.IntegerType),
      StructField("f2",DataTypes.IntegerType),
      StructField("f3",DataTypes.IntegerType)
    ))
    val test: DataFrame = spark.read.schema(schema2).csv("data/stu/input/test.txt")


    sample.createTempView("sample")
    test.createTempView("test")


    // 首先写一个普通的scala函数
    val dist = (arr1:Array[Int],arr2:Array[Int])=>{
      arr1.zip(arr2).map(tp=>Math.pow(tp._1-tp._2,2)).sum
    }

    spark.udf.register("dist",dist)

    val distDf = spark.sql(
      """
        |
        |select
        |
        |sample.id as sample_id,
        |sample.gender as sample_gender,
        |test.id,
        |dist(array(sample.f1,sample.f2,sample.f3),array(test.f1,test.f2,test.f3)) as dist
        |
        |from sample cross join test
        |
        |""".stripMargin)
    distDf.createTempView("dist_df")


    // TODO 距离算好，后续逻辑纯sql可以解决：
    // TODO 找到每个测试人距离最近的3个样本人，看这3个洋本人中，哪种性别最多，结果就是这种性别
    /**
     * dist_df
     * +---------+-------------+---+------+
       |sample_id|sample_gender|id |dist  |
       +---------+-------------+---+------+
       |1        |m            |a  |221.0 |
       |1        |m            |b  |874.0 |
       |2        |m            |a  |46.0  |
       |2        |m            |b  |1389.0|
       |3        |m            |a  |264.0 |
       |3        |m            |b  |973.0 |
       |4        |f            |a  |1406.0|
       |4        |f            |b  |59.0  |
       |5        |f            |a  |1668.0|
       |5        |f            |b  |21.0  |
       |6        |f            |a  |2001.0|
       |6        |f            |b  |4.0   |
       +---------+-------------+---+------+
     */
    spark.sql(
      """
        |select
        |  sample_id,
        |  sample_gender,
        |  id,
        |  rn
        |from (
        |select
        |  sample_id,
        |  sample_gender,
        |  id,
        |  row_number() over(partition by id order by dist) as rn
        |from dist_df ) o
        |where rn <=3
        |
        |
        |""".stripMargin).createTempView("knn")
    /**
     * knn
     * +---------+-------------+---+---+
       |sample_id|sample_gender|id |rn |
       +---------+-------------+---+---+
       |6        |f            |b  |1  |
       |5        |f            |b  |2  |
       |4        |m            |b  |3  |
       |2        |m            |a  |1  |
       |1        |f            |a  |2  |
       |3        |m            |a  |3  |
       +---------+-------------+---+---+
     */

    val res = spark.sql(
      """
        |select
        |   id,
        |   if(sum(if(sample_gender='f',0,1))>=2,'male','female') as gender
        |from knn
        |group by id
        |
        |""".stripMargin)

    res.show(100,false)

    spark.close()

  }


}
