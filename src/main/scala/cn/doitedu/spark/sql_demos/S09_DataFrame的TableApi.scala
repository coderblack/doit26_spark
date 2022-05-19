package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-12-04
 * @desc
 *
 * select
 *   sex,
 *   count(1)
 * from t1  join t2  on t1.id=t2.id
 * -- where id>10
 * group by sex
 * order by count(1)
 *
 */
object S09_DataFrame的TableApi {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("hive访问演示")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    //shop1,2019-01-18,500
    //shop1,2019-02-10,500
    val df: DataFrame = spark.read.csv("data/exersize/tm3/input")

    // 对一个df的字段进行重命名（全部字段都要写全）
    // val dfNamed: DataFrame = df.toDF("shopid", "dt", "amount")
      val dfNamed = df.selectExpr("_c0 as shopid","_c1 as dt","cast(_c2 as double) as amount")

    // 对某个指定的列进行改名
    val dfNamed2: DataFrame = dfNamed.withColumnRenamed("dt", "dtstr")

    df.printSchema()
    dfNamed.printSchema()
    dfNamed2.printSchema()

    /**
     * 一、查询api之 select
     * 相当于sql中的 "select id,name,age+10 from t"
     */
    val s1: DataFrame = dfNamed.select("shopid", "amount")
    //val s2: DataFrame = dfNamed.select("shopid", "amount*10")  // select这个方法中，传的列名字符串，只能是列名，不能是表达式

    //如果需要写表达式，可以传入Column类型的参数，把表达式运算用column对象的方法调用来表达
    //Column对象，可以直接用df的apply方法来构造
    dfNamed.select(dfNamed("shopid"),dfNamed("amount") * 10)  //

    // Column对象，也可以用 $"" 或者用 单边单引号 '  构造
    // 需要导入隐式转换
    import spark.implicits._
    dfNamed.select($"shopid",$"amount" * 10)  // $"amount" 等价于  dfNamed("amount")
    dfNamed.select('shopid,'amount * 10)  //  'shopid  等价于  $"amount" 等价于  dfNamed("amount")

    // Column对象，还可以用 col()函数
    // 需要导入col函数
    import org.apache.spark.sql.functions._
    dfNamed.select(col("shopid"),col("amount") * 10 as "amount2")

    /**
     * 其实，上面那些写法，都很麻烦，如果想在select中写表达式，直接换一个算子： selectExpr
     * 就可以直接写字符串表达式了
     */
    dfNamed.selectExpr("upper(shopid)"," (amount * 10) as amt")


    /**
     * where 过滤
     */
    dfNamed.where(" amount >= 500 ")
    dfNamed.where($"amount" > 500)


    /**
     * group  by  聚合
     */
    // shopid,sum(amount)
    dfNamed.groupBy("shopid").sum("amount")
    dfNamed.groupBy("shopid").agg(Map("amount"->"sum","amount"->"max"))  // avg,min,max,sum,count

    dfNamed.groupBy("shopid").agg(
      collect_list("amount") as "col_lst",
      collect_set("amount") as "col_set",
      sum("amount") as "sum_amt",
      count(lit(1)) as "cnt",
      min("amount") as "min_amt",
      max("amount") as "max_amt",
      avg("amount") as "avg_amt"
    )


    /**
     * 窗口函数
     * 求店铺月累计金额
     */
    val tmp = dfNamed
      .groupBy($"shopid",month($"dt") as "mth")  // groupby中的表达式命名后，结果中就已经命名了
      .agg(sum("amount") as "mth_amt")

    val spec: WindowSpec = Window.partitionBy("shopid").orderBy("mth").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val res: DataFrame = tmp.select($"shopid", $"mth", $"mth_amt", sum("mth_amt") over spec as "accu_amt")

    /**
     * join
     */
    val df2 = spark.read.csv("data/exersize/tm3/shopinfo")
      .selectExpr("_c0 as shopid","cast(_c1 as double) as bzj","_c2 as shop_name")

    // 关联 dfNamed 和 df2  ，按 shopid=shopid
    dfNamed.crossJoin(df2) // 笛卡尔积
    dfNamed.join(df2) // 没有加条件的inner join，相当于笛卡尔积
    dfNamed.join(df2,Seq("shopid"))   // usingColumn形式写join条件，前提：join条件字段在两表中名字相同；如果join条件字段有多个，则用Seq封装
    dfNamed.join(df2,dfNamed("shopid") === df2("shop_id")) // join条件字段在两表中名字不同，则可以用Column参数来表达

    // 指定join的类型
    //joinType – 默认(inner) ，其他可写的是: inner, cross, outer, full, fullouter, full_outer, left, leftouter, left_outer, right, rightouter, right_outer, semi, leftsemi, left_semi, anti, leftanti, left_anti.
    dfNamed.join(df2,Seq("shopid"),"left")


    /**
     * union
     */
    val df03: DataFrame = spark.read.csv("data/acc_log/2021-12-03")
    val df04: DataFrame = spark.read.csv("data/acc_log/2021-12-04")
    // table api中的union就是union all，并不会去重
    val df5: Dataset[Row] = df03.union(df04)


    /**
     * 子查询
     */
    val tmp2 = dfNamed.where("amount > 500")
    tmp2.select("shopid","amount")  // 在前一个算子的结果至上，再调别的sql算子，就是子查询


    spark.close()
  }
}
