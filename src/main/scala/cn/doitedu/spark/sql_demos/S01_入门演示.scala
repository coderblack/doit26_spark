package cn.doitedu.spark.sql_demos

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object S01_入门演示 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("sparksql入门演示")
      .getOrCreate()

    //
    //val df: DataFrame = spark.read.option("header", "true").csv("data/battel/input")

    import  spark.implicits._

    val df = spark.createDataset(Seq((1,"a"),(2,"b"))).toDF("id","name")


    df.createTempView("t1")

    val res: DataFrame = spark.sql(
      """
        |
        |select id,name
        |
        |from t1
        |where id>10
        |
        |""".stripMargin)
    res.show(100)





    spark.close()

  }
}
