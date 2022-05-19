package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object S02_DataFrame创建之CSV文件 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("csv创建dataframe")
      .master("local")
      .config("spark.default.parallelism", 20)
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()


    val df1: DataFrame = spark.read.csv("data/csv/input/b.csv")
    df1.show(100,false)
    df1.printSchema()

    println("------------------------")

    val df2: DataFrame = spark.read.option("header","true").csv("data/csv/input/a.csv")
    df2.show(100,false)
    df2.printSchema()

    println("------------------------")


    // 自动推断字段类型：  "inferSchema" -> "true"
    val df3: DataFrame = spark.read.option("header","true").option("inferSchema","true").csv("data/csv/input/a.csv")
    df3.show(100,false)
    df3.printSchema()


    // 手动指定schema（字段名+字段类型）
    // 构造一个表结构描述对象
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("age",DataTypes.IntegerType),
      StructField("income",DataTypes.DoubleType),
    ))

    val df4: DataFrame = spark.read.option("header","true").schema(schema).csv("data/csv/input/a.csv")
    df4.show(100,false)
    df4.printSchema()

    spark.close()
  }

}
