package cn.doitedu.spark.demos

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .config("spark.sql.shuffle.partitions","1")
      .master("local")
      .getOrCreate()

    import  spark.implicits._
    val df: DataFrame = spark.createDataset(Seq(1, 2, 3, 4, 5)).toDF("id")


    df.show(100,false)

    /*df.createTempView("df")

    spark.sql(
      """
        |
        |select id from df where id<4 and id < 3
        |
        |""".stripMargin).show(100)*/


    spark.close()
  }

}
