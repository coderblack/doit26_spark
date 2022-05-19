import org.apache.commons.lang3.time.DateUtils

import java.util.Date

object ScanTest {

  def main(args: Array[String]): Unit = {
    val lst = List(1,2,3,4,5,6,7,8)

    val ints: List[Int] = lst.scan(0)(_ + _).tail


    println(ints)



    val inInterval = (dt1:String,dt2:String)=>{
      val d1: Date = DateUtils.parseDate(dt1, "yyyy-MM-dd HH:mm:ss")
      val d2: Date = DateUtils.parseDate(dt2, "yyyy-MM-dd HH:mm:ss")
      d2.getTime - d1.getTime <= 10*60*1000
    }

    println(inInterval("2020-02-18 14:20:00", "2020-02-18 14:25:01"))

  }
}
