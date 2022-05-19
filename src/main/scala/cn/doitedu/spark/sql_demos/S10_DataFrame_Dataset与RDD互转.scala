package cn.doitedu.spark.sql_demos

import cn.doitedu.sparksql.JavaPerson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

case class Per(id: Int, name: String, age: Int)

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-12-04
 * @desc rdd -> ds / df
 *       sparkSession.createDataFrame / creaetDataset
 */
object S10_DataFrame_Dataset与RDD互转 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("互转")
      .master("local")
      .getOrCreate()



    /**
     * 一 、 把rdd变成dataset或dataframe
     */
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Seq((1, "zs", 18), (2, "ls", 19), (3, "ww", 20)))

    // 把已存在的rdd，转成dataframe/dataset
    // sparksession拥有工具方法，来将普通的rdd，转成dataset和dataframe，也可以将普通的单机集合数据转成dataset
    // spakrsession将一个rdd转成dataset时，需要传入一个Encoder（编码器）：用于从用户rdd的元素对象中通过反射方式生成schema；以及数据对象的序列化器
    // 将RDD[T]转成Dataset，就需要相对应的 Encoder[T]
    // spark中内置了大量的常用类型的对应 Encoder，可以手动通过Encoders来选择即可
    spark.createDataset(rdd)(Encoders.tuple(Encoders.scalaInt, Encoders.STRING, Encoders.scalaInt))

    // 而Encoder参数被定义成了隐式参数，所以我们通常是让它自动隐式传入
    import spark.implicits._
    val ds: Dataset[(Int, String, Int)] = spark.createDataset(rdd)
    ds.printSchema()


    // dataset上只要执行了sql算子，就会退化成dataframe
    val y: DataFrame = ds.selectExpr("_1 as id", "_2 as name", "_3 as age", " '2021-12-04' as dt")
    val x: DataFrame = ds.toDF("id", "name", "age")

    // 元组类型rdd，可以直接转成 dataframe
    val df: DataFrame = spark.createDataFrame(rdd)
    df.printSchema() // _1,  _2,  _3

    // case class 类型rdd，可以直接转成 dataframe
    val personRDD = rdd.map(tp => Per(tp._1, tp._2, tp._3))
    val df2: DataFrame = spark.createDataFrame(personRDD)
    df2.printSchema()

    // java bean 类型的rdd，需要指定bean的class类型
    val personBeanRdd: RDD[JavaPerson] = rdd.map(tp => new JavaPerson(tp._1, tp._2, tp._3))
    val df3: DataFrame = spark.createDataFrame(personBeanRdd, classOf[JavaPerson])
    df3.printSchema()

    // scala bean 类型的rdd
    val scalaStuRdd: RDD[ScalaStu] = rdd.map(tp => new ScalaStu(tp._1, tp._2, tp._3))
    val df4: DataFrame = spark.createDataFrame(scalaStuRdd, classOf[ScalaStu])
    df4.printSchema()


    /**
     * 二、 把dataset或dataframe 转成 RDD
     */
    // ds 转回 rdd，会保留原来的元素的类型
    val rdd1: RDD[(Int, String, Int)] = ds.rdd
    // dataframe 转成rdd，一定是RDD[Row] ,因为： DataFrame 就是Dataset[Row]
    val rdd2: RDD[Row] = df4.rdd

    /**
     * 三、 把 dataset和 dataframe 之间的互转
     */
    // dataset -> dataframe , 底层是： 具体类型->Row
    val frame: DataFrame = ds.toDF("id", "name", "age")



    // dataframe -> dataset[]  ,底层是：  Row -> 具体类型，需要Encoder
    // 把dataframe转成  caseclass类型的dataset，会自动传入Encoder
    val ds2: Dataset[Per] = frame.as[Per]

    // 把dataframe转成java对象的dataset，则需要手动传Encoder
    val ds3: Dataset[JavaPerson] = frame.as[JavaPerson](Encoders.bean(classOf[JavaPerson]))


    /**
     * 四、 dataset/dataframe 调 RDD算子
     *
     * dataset调rdd算子，返回的还是dataset[U], 不过需要对应的 Encoder[U]
     *
     */
    val ds4: Dataset[(Int, String, Int)] = ds2.map(p => (p.id, p.name, p.age + 10))   // 元组有隐式Encoder自动传入
    val ds5: Dataset[JavaPerson] = ds2.map(p => new JavaPerson(p.id,p.name,p.age*2))(Encoders.bean(classOf[JavaPerson])) // Java类没有自动隐式Encoder，需要手动传

    val ds6: Dataset[Map[String, String]] = ds2.map(per => Map("name" -> per.name, "id" -> (per.id+""), "age" -> (per.age+"")))
    ds6.printSchema()
    /**
     * root
         |-- value: map (nullable = true)
         |    |-- key: string
         |    |-- value: string (valueContainsNull = true)
     */
    // 从ds6中查询每个人的姓名
    ds6.selectExpr("value['name']")


    // dataframe上调RDD算子，就等价于 dataset[Row]上调rdd算子
    val ds7: Dataset[(Int, String, Int)] = frame.map(row=>{
      val id: Int = row.getInt(0)
      val name: String = row.getAs[String]("name")
      val age: Int = row.getAs[Int]("age")
      (id,name,age)
    })


    /*frame.map(row=>{
      row match {
        case Row(id:Int,name:String,age:Int) => Per(id,name,age*10)
      }
    })*/

    // 利用模式匹配从row中抽取字段数据
    val ds8: Dataset[Per] = frame.map({
      case Row(id:Int,name:String,age:Int) => Per(id,name,age*10)
    })


    spark.close()

  }

}
