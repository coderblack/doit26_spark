object Scala闭包 {


  def main(args: Array[String]): Unit = {

    var a = 100

    val f = (i: Int) => {
      val res = 10 + i + a;
      a = 500;
      res
    }
    println(f(10))
    println(a)


    println("---------------------------")


    a = 200
    println(f(10))


  }


}
