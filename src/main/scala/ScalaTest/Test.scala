package ScalaTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName(this.getClass.toString().dropRight(1))
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    
    val data=Range(1,100)
    val rdd = sc.parallelize(data, 2)
 
    rdd.take(10).foreach { x => println(x) }
    
    sc.stop()
  }
}