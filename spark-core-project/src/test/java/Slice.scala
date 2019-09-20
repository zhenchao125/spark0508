/**
  * Author lzc
  * Date 2019-09-20 09:26
  */
object Slice {
    def main(args: Array[String]): Unit = {
        val arr1 = List(30, 50, 70, 60, 10, 20)
        /*val prePages = arr1.slice(0, arr1.length - 1)
        val postPages = arr1.slice(1, arr1.length)*/
        val prePages = arr1.take(arr1.length - 1)
        val postPages = arr1.tail
        println(prePages)
        println(postPages)
    }
}
