import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

import java.util.Date

object RowNumber {
  def main(args: Array[String]): Unit = {

    val lst = List(
      ("guid01", "2018-02-28"),
      ("guid01", "2018-03-01"),
      ("guid01", "2018-03-02"),
      ("guid01", "2018-03-04"),
      ("guid01", "2018-03-05"),
      ("guid01", "2018-03-06"),
      ("guid01", "2018-03-07")
    )


    val res = lst.zipWithIndex.map(tp=>{
      val dtSub: Date = DateUtils.addDays(DateUtils.parseDate(tp._1._2, "yyyy-MM-dd"), -tp._2)
      (DateFormatUtils.format(dtSub,"yyyy-MM-dd"),tp._1)
    })

    res.groupBy(tp=>tp._1).mapValues(lst=>{
     val  lst2 = lst.sortBy(_._2._2)
      (lst2.size,lst2.head._2._2,lst2.reverse.head._2._2)
    }).map(tp=>tp._2).foreach(println)

  }

}
