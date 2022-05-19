package cn.doitedu.spark.util

import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

object BitMapUtil {

  def serBitMap(bm:RoaringBitmap):Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val dataOutput = new DataOutputStream(stream)
    bm.serialize(dataOutput)

    stream.toByteArray
  }


  def deSerBitMap(byteArray:Array[Byte]):RoaringBitmap = {

    val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf()

    val stream = new ByteArrayInputStream(byteArray)
    val dataInput = new DataInputStream(stream)

    bitmap.deserialize(dataInput)

   bitmap
  }


}
