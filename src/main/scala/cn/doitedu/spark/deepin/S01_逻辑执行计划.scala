package cn.doitedu.spark.deepin

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object S01_逻辑执行计划 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("逻辑执行计划")
      .getOrCreate()

    import spark.implicits._
    spark.createDataset(Seq((1,"a",19),(2,"b",20))).toDF("id","name","age").createTempView("t1")
    spark.createDataset(Seq((1,"138","sh"),(2,"136","bj"))).toDF("id","phone","addr").createTempView("t2")


    val sql =
      """
        |select
        |  o1.id,
        |  o1.name,
        |  o1.age,
        |  o2.phone,
        |  o2.addr
        |from
        |(select id,name,age from t1 where id is not null) o1
        |join
        |(select id,phone,addr from t2 where id is not null) o2
        |on o1.id=o2.id
        |where addr!='上海'
        |""".stripMargin


    val plan: LogicalPlan = spark.sessionState.sqlParser.parsePlan(sql)

    println(plan)


    val df: DataFrame = spark.sql(sql)
    df.show(100,false)

    df.explain("codegen")

    spark.close()

  }
}
