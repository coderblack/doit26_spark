package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object S05_DataFrame创建之Parquet文件 {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName("Parquet文件创建dataframe")
      .master("local")
      .config("spark.default.parallelism", 20)
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()


    // id,name,age,salary
    val schema = StructType(Seq(
      StructField("id",DataTypes.LongType),
      StructField("name",DataTypes.StringType),
      StructField("age",DataTypes.IntegerType),
      StructField("salary",DataTypes.DoubleType),
    ))
    val df: DataFrame = spark.read.option("header", "true").schema(schema).csv("data/csv/input/a.csv")

    df.write.parquet("data/parquet/")
    df.write.parquet("data/orc/")


    // 读取上面job生成的parquet文件  // parquet是自我描述的列式存储文件格式
    val df2: DataFrame = spark.read.parquet("data/parquet")
    df2.show(100,false)
    df2.printSchema()

    // 读取上面job生成的orc文件  // orc也是自我描述的列式存储文件格式
    val df3: DataFrame = spark.read.parquet("data/orc")
    df3.show(100,false)
    df3.printSchema()


    spark.close()

  }

}
