import java.text.DecimalFormat

/**
  * Author lzc
  * Date 2019-09-20 10:23
  */
object DecimalFormaDemo {
    def main(args: Array[String]): Unit = {
        val formater = new DecimalFormat(".00%")
        println(formater.format(10.236))
    }
}
