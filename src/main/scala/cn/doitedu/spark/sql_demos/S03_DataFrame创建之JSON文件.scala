package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object S03_DataFrame创建之JSON文件 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("json创建dataframe")
      .master("local")
      .config("spark.default.parallelism", 20)
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("data/json/input/a.txt")

    df.show(100, false)
    df.printSchema()

    /** 解析失败的行，会在记录在一个专门的额外字段中： _corrupt_record
     * +--------------------------------------------+----+----+----+------+
     * |_corrupt_record                             |age |id  |name|sex   |
     * +--------------------------------------------+----+----+----+------+
     * |null                                        |18  |1   |aaa |male  |
     * |{"id":2,"sex":"male","name":"bbb","age":}   |null|null|null|null  |
     * |{"id":3,"sex":"male","name":"ccc","age":38  |null|null|null|null  |
     * |null                                        |16  |4   |ddd |female|
     * |{"id":5,"sex":"female","name":eee","age":28}|null|null|null|null  |
     * |null                                        |16  |6   |fff |female|
     * +--------------------------------------------+----+----+----+------+
     */

    // 筛选出脏数据
    import org.apache.spark.sql.functions._
    df.filter($"_corrupt_record".isNull)


    // ---------------------- 演示 复杂嵌套json的读取  ----------------------
    val df2: DataFrame = spark.read.json("data/json/input/b.txt")
    df2.show(100, false)
    df2.printSchema()

    /**
     * +---------------------------------------------+---+----+
     * |family                                       |id |name|
     * +---------------------------------------------+---+----+
     * |[{mother, 111}, {father, 222}, {brother, zz}]|1  |zs  |
     * |[{mother, tt}, {father, ii}, {brother, ww}]  |2  |bb  |
     * |[{mother, yy}, {father, oo}]                 |3  |cc  |
     * |[{mother, uu}, {father, pp}, {sister, qq}]   |4  |dd  |
     * +---------------------------------------------+---+----+
     * root
     * |-- family: array (nullable = true)
     * |    |-- element: struct (containsNull = true)
     * |    |    |-- guanxi: string (nullable = true)
     * |    |    |-- name: string (nullable = true)
     * |-- id: long (nullable = true)
     * |-- name: string (nullable = true)
     */

    df2.createTempView("df2")
    // 取每个人的mother
    spark.sql(
      """
        |
        |select
        |   family[0].name
        |from df2
        |
        |""".stripMargin).show(100, false)

    // ---------------------- 演示 复杂嵌套json的读取  ----------------------
    val df3: DataFrame = spark.read.json("data/json/input/c.txt")
    df3.show(100, false)
    df3.printSchema()

    /** 解析出来的结果比较丑陋，info被认成了struct类型，而struct类型中的成员变量是统一
     * +---+---------------------------------------+
     * |id |info                                   |
     * +---+---------------------------------------+
     * |1  |{null, 18, null, a, null}              |
     * |2  |{null, 18, male, b, null}              |
     * |3  |{上海, null, male, c, null}            |
     * |4  |{null, null, female, 楚龙, 13876543219}|
     * +---+---------------------------------------+
     *
     * root
     * |-- id: long (nullable = true)
     * |-- info: struct (nullable = true)
     * |    |-- addr: string (nullable = true)
     * |    |-- age: long (nullable = true)
     * |    |-- gender: string (nullable = true)
     * |    |-- name: string (nullable = true)
     * |    |-- phone: string (nullable = true)
     */

    println("----------------------神一般的男人---------------------")

    // 手动指定schema来改善上面的问题
    val schema = StructType(Seq(
      StructField("id", DataTypes.LongType),
      StructField("info", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
    ))

    val df4: DataFrame = spark.read.schema(schema).json("data/json/input/c.txt")
    df4.show(100, false)
    df4.printSchema()

    println("----------------------神经一般的男人---------------------")
    // 找出有年龄属性的数据后，求平均年龄
    df4.createTempView("f4")
    spark.sql(
      """
        |
        |select
        |avg(info['age']) as avg_age
        |from f4
        |where info['age'] is not null
        |
        |""".stripMargin).show(100,false)


    spark.close()
  }

}
