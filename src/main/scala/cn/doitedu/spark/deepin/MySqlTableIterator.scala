package cn.doitedu.spark.deepin

import java.sql.{Connection, DriverManager, ResultSet, Statement}


case class Soldier(id:Int,name:String,role:String,fight:Double)

class MySqlTableIterator(url:String,username:String,password:String,tableName:String) extends Iterator[Soldier]{

  private val conn: Connection = DriverManager.getConnection(url, username, password)
  private val stmt: Statement = conn.createStatement()
  private val resultSet: ResultSet = stmt.executeQuery(s"select * from ${tableName}")

  override def hasNext: Boolean = {
    resultSet.next()
  }

  override def next(): Soldier = {
    val id: Int = resultSet.getInt("id")
    val name: String = resultSet.getString("name")
    val role: String = resultSet.getString("role")
    val battel: Double = resultSet.getDouble("battel")

    Soldier(id,name,role,battel)
  }
}

class MySqlTableIterable(url:String,username:String,password:String,tableName:String) extends Iterable[Soldier] {
  override def iterator: Iterator[Soldier] = new MySqlTableIterator(url,username,password,tableName)
}


object MySqlTableIteratorTest{
  def main(args: Array[String]): Unit = {

    val iter = new MySqlTableIterable("jdbc:mysql://localhost:3306/abc", "root", "123456", "battel")

    // 统计每种角色的总战斗力
    val res = iter.map(soldier=>(soldier.role,soldier.fight))
      .groupBy(_._1)
      .mapValues(iter=>iter.map(_._2).sum)


    res.foreach(println)




  }
}



