package cn.doitedu.spark.sql_demos

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

object S11_DataFrame的输入输出总结 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("输入输出")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    // 读csv文件  --schame：逗号切开，有几个就有几列，字段名：_c0 :String
    val df1: DataFrame = spark.read.csv("")

    // 读json文件 --schema: json中的key是字段名，value就是json中的value
    val df2: DataFrame = spark.read.json("")

    // 读text文件  -- schema：  value:String
    val df3: DataFrame = spark.read.text("")

    // 读parquet文件  -- schema：  parquet自带的schema
    val df4: DataFrame = spark.read.parquet("")

    // 读orc文件  -- schema：  orc自带的schema
    val df5: DataFrame = spark.read.orc("")


    // 读mysql   --schema ：mysql中的表schema
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val df6: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/dbname", "tableName", props)

    // 读hive  --schema ：hive中的表schema
    val df7: DataFrame = spark.read.table("db.tableName")

    // -- schema : select的结果schema
    val df8: DataFrame = spark.sql(
      """
        |select
        |  id,name,age
        |from db.tableName
        |
        |""".stripMargin)


    // 写csv文件
    df8.write.csv("")

    // 写json文件
    df8.write.json("")

    // 写text文件
    df8.write.text("")

    // 写mysql
    df8.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/db","tableName",props)

    // 写hive
    df8.write.mode(SaveMode.Overwrite).saveAsTable("db.tableName")

    // table api风格  运算
    df8.selectExpr("id","age+10")  //

    // 纯sql风格 运算
    df8.createTempView("df8")
    spark.sql(
      """
        |
        |select
        |  id,name
        |from df8
        |
        |""".stripMargin)

    // rdd 风格api
    import spark.implicits._
    val ds: Dataset[(String, Int)] = df8.map(row=>(row.getAs[String]("name"),row.getAs[Int]("age")))

    // 将dataset（dataframe）转成rdd  再做计算
    val rdd: RDD[(String, Int)] = df8.rdd.map(row => (row.getAs[String]("name"), row.getAs[Int]("age")))

  }
}
