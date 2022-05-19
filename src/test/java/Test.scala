object Test {
  def main(args: Array[String]): Unit = {

    val arr1 = Array("","b")
    val arr2 = Array("   ","b")

    println(arr1.contains(""))
    println(arr2.contains("\\s+"))


    val url = "http://baidu.com/abc/x"

    val x = "(?:https?://)?(?:www\\.)?([A-Za-z0-9._%+-]+)/?.*".r

    url match {
      case x(d) => println(d)
      case _ => println("none")
    }


  }

}
