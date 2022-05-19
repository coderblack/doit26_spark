package cn.doitedu.spark.deepin

import java.io.{BufferedReader, FileReader}

class MyFileIterator(filePath:String) extends Iterator[String]{

  private val br = new BufferedReader(new FileReader(filePath))

  var line: String = null

  override def hasNext: Boolean = {
    line  = br.readLine()
    line !=null
  }

  override def next(): String = {
    line
  }
}

class MyFileIterable(filePath:String)  extends Iterable[String]{
  override def iterator: Iterator[String] = new MyFileIterator(filePath)
}


object MyFileIteratorTest {
  def main(args: Array[String]): Unit = {
    val iterable: Iterable[String] = new MyFileIterable("data/wordcount/input/a.txt")

    /*val iter: Iterator[String] = iterable.iterator
    while(iter.hasNext){
      val e: String = iter.next()
      println(e)
    }*/

    println("-----------------分割线--------------")

    //wordcount
    val res = iterable.flatMap(s => s.split("\\s+")).map(s => (s, 1)).groupBy(tp=>tp._1).mapValues(iter=>iter.size)
    res.foreach(println)
  }

}