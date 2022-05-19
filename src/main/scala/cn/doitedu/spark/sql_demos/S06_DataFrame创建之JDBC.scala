package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object S06_DataFrame创建之JDBC {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("S06_DataFrame创建之JDBC")
      .master("local")
      .getOrCreate()


    // 通过jdbc来连接mysql得到dataframe
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/abc", "battel_info", props)

    df.show(100,false)
    df.printSchema()


    spark.close()


  }

}
