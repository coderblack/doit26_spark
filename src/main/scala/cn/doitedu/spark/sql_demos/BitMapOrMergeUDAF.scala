package cn.doitedu.spark.sql_demos

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap
import cn.doitedu.spark.util.BitMapUtil.serBitMap
import cn.doitedu.spark.util.BitMapUtil.deSerBitMap

object BitMapOrMergeUDAF extends Aggregator[Array[Byte],Array[Byte],Array[Byte]]{
  override def zero: Array[Byte] = {
    // 构造一个空bitmap
    val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()
    serBitMap(bitmap)
  }

  override def reduce(bm1: Array[Byte], bm2: Array[Byte]): Array[Byte] = {
    val bitmap1: RoaringBitmap = deSerBitMap(bm1)
    val bitmap2: RoaringBitmap = deSerBitMap(bm2)
    bitmap1.or(bitmap2)

    serBitMap(bitmap1)
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    reduce(b1,b2)
  }

  override def finish(reduction: Array[Byte]): Array[Byte] = reduction

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
