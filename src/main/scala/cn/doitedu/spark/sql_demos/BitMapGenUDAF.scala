package cn.doitedu.spark.sql_demos

import cn.doitedu.spark.util.BitMapUtil.serBitMap
import cn.doitedu.spark.util.BitMapUtil.deSerBitMap
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap

object BitMapGenUDAF extends Aggregator[Int,Array[Byte],Array[Byte]]{
  // 初始化中间缓存结构
  override def zero: Array[Byte] = {
    // 构造一个空的bitmap
    val bm: RoaringBitmap = RoaringBitmap.bitmapOf()

    // 序列化这个空的bitmap交还给框架
    serBitMap(bm)
  }

  // 局部聚合逻辑
  override def reduce(b: Array[Byte], a: Int): Array[Byte] = {
    // 将序列化后的buff反序列化成 RoaringBitMap对象
    val bitmap: RoaringBitmap = deSerBitMap(b)

    // 向缓存bitmap中添加新元素  a
    bitmap.add(a)

    // 再将更新后的缓存序列化成字节数组返回
    serBitMap(bitmap)

  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val bitmap1: RoaringBitmap = deSerBitMap(b1)
    val bitmap2: RoaringBitmap = deSerBitMap(b2)

    // 合并两个bitmap（或操作）
    bitmap1.or(bitmap2)

    serBitMap(bitmap1)
  }

  override def finish(reduction: Array[Byte]): Array[Byte] = {
    reduction
  }

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
