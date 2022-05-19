package cn.doitedu.spark.deepin

object IteratorTest {
  def main(args: Array[String]): Unit = {

    val lst = List(1, 2, 3, 4, 5)

    val iter1: Iterator[Int] = lst.iterator


    /**
     * scala迭代器算子的lazy特性
     * 相当于rdd上的transformation算子
     */
    val iter2 = iter1.map(e => {
      println("函数1被执行了......")
      e + 10
    })


    val iter3 = iter2.map(e => {
      println("函数2被执行了.....");
      e * 100
    })

    //iter3 :
    // hasNext() = iter2.hasNext => iter1.hasNext
    // next() = f2(iter2.next(f1(iter1.next())))


    // 相当于rdd中的action算子
    iter3.foreach(println)

  }


}
