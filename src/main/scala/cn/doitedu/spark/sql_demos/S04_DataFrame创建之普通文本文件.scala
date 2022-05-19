package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * sparksql将输入数据视作非结构化数据来读的时候
 * 就是把整行内容当成一个字段（value:String)
 * 接下去你可以用纯sql来计算
 * 也可以用rdd的api来计算
 *
 */
object S04_DataFrame创建之普通文本文件 {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName("普通text文件创建dataframe")
      .master("local")
      .config("spark.default.parallelism", 20)
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()


    // dataframe 就是dataset的一个特例：  Dataset[Row]
    // type DataFrame = Dataset[Row]
    val df: DataFrame = spark.read.text("data/wordcount/input")
    df.show(100,false)
    df.printSchema()  // value:String


    println("--------------不可思议的男人--------------")

    val ds: Dataset[String] = spark.read.textFile("data/wordcount/input")
    ds.show(100,false)
    ds.printSchema()

    println("--------------不可思议的男人--------------")
    // 做wordcount
    df.createTempView("df")
    val words = spark.sql(
      """
        |select
        |word,count(1) as cnt
        |from
        |(
        |   select
        |      explode(split(value,'\\s+')) as word
        |   from df
        |) o
        |group by word
        |
        |""".stripMargin)
    words.show(100,false)
    words.printSchema()


    import spark.implicits._
    val ds2 :Dataset[Row] = df.flatMap(row=>{
      val line: String = row.getAs[String]("value")
      line.split("\\s+")
    }).toDF("word")

    ds2.createTempView("ds2")
    spark.sql(
      """
        |select
        |  word,count(1) as cnt
        |from ds2
        |group by word
        |
        |""".stripMargin)


    spark.close()

  }

}
