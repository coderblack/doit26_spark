package cn.doitedu.spark.sql_demos

import scala.beans.BeanProperty

class ScalaStu(
                @BeanProperty  // 帮助自动生成 id字段的getter和setter
                val id: Int,
                @BeanProperty
                val name: String,
                @BeanProperty
                val age: Int) {
}
