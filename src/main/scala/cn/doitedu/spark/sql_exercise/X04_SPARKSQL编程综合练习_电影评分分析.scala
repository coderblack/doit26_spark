package cn.doitedu.spark.sql_exercise

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-12-04
 * @desc
 *   -----1.加载解析原始电影评分相关数据，得到表并存入hive
 *      1::Toy Story (1995)::Animation|Children's|Comedy
 *      --> 要求的表结构   movie_id bigint,  movie_name string,  style array<string>
 *      1::1836::5::978300172
 *      --> 要求的表结构   user_id bigint,  movie_id bigint,  rate int,  ts bigint
 *      1::F::1::10::48067
 *      --> 要求的表结构   user_id bigint,  gender string,  age  int
 *      -->  并且过滤掉年龄<10岁的数据
 *   ------2.对存入hive中的上面3个表，做数据统计分析
 *       报表1： 平均得分最高的100部电影
 *       报表2:  评分次数最高的100部电影
 *       报表3： 每种风格下，平均得分最高的前10部电影
 *       报表4： 每种性别的观众，观看最多的前3种电影风格
 */
object X04_SPARKSQL编程综合练习_电影评分分析 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("电影数据分析")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    // 加载电影信息数据
    val movies: DataFrame = spark.read.option("sep", "::").csv("file:///D:\\devworks\\doit26_spark\\data\\movie\\movies.dat")
    val ratings: DataFrame = spark.read.option("sep", "::").csv("file:///D:\\devworks\\doit26_spark\\data\\movie\\ratings.dat")
    val users: DataFrame = spark.read.option("sep", "::").csv("file:///D:\\devworks\\doit26_spark\\data\\movie\\users.dat")

    val tableMovie: DataFrame = movies.selectExpr(
      "cast(_c0 as bigint) as movie_id",
      "_c1 as movie_name",
      "split(_c2,'\\\\|') as style"
    )

    val tableRating: DataFrame = ratings.selectExpr(
      "cast(_c0 as bigint) as user_id",
      "cast(_c1 as bigint) as movie_id",
      "cast(_c2 as int) as rate",
      "cast(_c3 as bigint) as ts"
    )


    val tableUser: DataFrame = users.selectExpr(
      "cast(_c0 as bigint) as user_id",
      "cast(_c1 as string) as gender",
      "cast(_c2 as int) as age"
    ).where("age >= 10")

    tableMovie.write.saveAsTable("movie.movies")
    tableRating.write.saveAsTable("movie.ratings")
    tableUser.write.saveAsTable("movie.users")





    spark.stop()

  }

}
