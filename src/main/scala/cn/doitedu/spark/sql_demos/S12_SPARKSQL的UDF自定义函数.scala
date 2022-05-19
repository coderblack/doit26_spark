package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-12-05     
 * @desc 自定义UDF示例
 */
object S12_SPARKSQL的UDF自定义函数 {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName("自定义函数demo")
      .master("local")
      .getOrCreate()


    val df: DataFrame = spark.read
      .option("header", "true")
      .csv("file:///D:\\devworks\\doit26_spark\\data\\battel\\input\\a.txt")
      .toDF("id","country","name","battle","age")

    df.createTempView("df")

    val func = (c:String,n:String)=>{
      val firstName: String = n.substring(0, 1)
      val lastName: String = n.substring(1)
      firstName+c+lastName
    }

    // 往sparksql的catalog中，注册函数名
    spark.udf.register("qiguai",func)


    // id,country,name,battle,age
    val res: DataFrame = spark.sql(
      """
        |select
        |  id,country,name,battle,age,qiguai(country,name) as new_name
        |from df
        |
        |""".stripMargin)

    res.show(100,false)


    spark.close()

  }

}
