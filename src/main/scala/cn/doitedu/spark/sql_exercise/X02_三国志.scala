package cn.doitedu.spark.sql_exercise

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.util.Properties

/**
 * 将文件：  data/battel/input/a.txt  和  mysql中的表： batttel_info 的数据进行关联
 * 然后统计  ： 每个城市战斗力最高的两位将军的所有信息；
 *            总战斗力最高的前2座城市
 */
object X02_三国志 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("三国演义")
      .master("local")
      .config("spark.sql.shuffle.partitions",2)
      .getOrCreate()


    // 加载文本文件数据
    // id,country,name,battle,age
    val schema = StructType(
      Seq(
        StructField("id",DataTypes.LongType),
        StructField("country",DataTypes.StringType),
        StructField("name",DataTypes.StringType),
        StructField("battle",DataTypes.DoubleType),
        StructField("age",DataTypes.IntegerType)
      )
    )
    val df1: DataFrame = spark.read.option("header", "true").schema(schema).csv("data/battel/input/a.txt")

    // 加载mysql中的表
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val df2: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/abc", "battel_info", props)


    // 将两个dataframe分别注册视图
    df1.createTempView("df1")
    df2.createTempView("df2")

    // 因为后面的两个需求都需要用到df1 join df2，所以干脆先存一个join好的表
    val joined = spark.sql(
      """
        |select
        |  df1.id,
        |  df1.country,
        |  df1.name,
        |  df1.battle,
        |  df1.age,
        |  df2.phone,
        |  df2.city,
        |  row_number() over(partition by df2.city order by df1.battle desc) as rn
        |from df1 join df2  on df1.id=df2.id
        |
        |""".stripMargin)
    joined.createTempView("t")


    // 写sql实现需求1 : 每个城市战斗力最高的两位将军的信息   -- 分组topn
    val res1 = spark.sql(
      """
        |select
        |  id,
        |  country
        |  name,
        |  battle,
        |  age,
        |  phone,
        |  city
        |from t
        |where rn<=2
        |
        |""".stripMargin)
    res1.show(100,false)


    println("-----------------帅的无边无际的男人--------------------")

    // 写sql实现需求2 : 总战斗力最高的前两座城市   -- 全局topn
    val res2 = spark.sql(
      """
        |
        |select
        |  city,
        |  sum(battle) as battle_amt
        |from  t
        |group by city
        |order by battle_amt desc
        |limit 2
        |
        |
        |""".stripMargin)

    res2.show(100,false)

    spark.close()


  }
}
