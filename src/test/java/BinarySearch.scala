import java.util
import java.util.Collections

object BinarySearch {
  def binarySearch(lst:List[Int],number:Int):Int = {
    var low = 0;
    var high = lst.size-1;

    while(low<=high){
      val mid = (low+high)/2
      if(lst(mid)==number){
        return mid
      }else if(lst(mid)>number){
        high = mid-1
      }else {
        low = mid +1
      }
    }
    return -1
  }


  def main(args: Array[String]): Unit = {

    val lst = List(1,2,3,4,5,6,8,19,20,32)
    val number = 32
    val idx: Int = binarySearch(lst, number)
    println(idx)
  }

}
