import scala.reflect.ClassTag

object Test {
    def main(args: Array[String]): Unit = {
        foo("")
    }
    def foo[T: ClassTag](a:T) = {
        val arr = Array[T](a)
    }
    
    
    
    
}
