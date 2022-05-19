package org.apache.spark

import org.apache.spark.rdd.RDD

object CheckPointLoader {

  def load(sc:SparkContext,path:String): RDD[(String, Int)] ={
    sc.checkpointFile[(String,Int)](path)
  }


}
