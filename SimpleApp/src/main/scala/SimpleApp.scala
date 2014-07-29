/**
 * Created by wanghao on 6/12/14.
 */

package ca.haow.www

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("spark://sing12:7077")
    val sc = new SparkContext(conf)
    sc.addJar("/home/wanghao/Documents/Scala/SimpleApp/target/scala-2.10/simple-project_2.10-1.0.jar")

    val file = sc.textFile(args(0)) // Should be some file on your system
    val counts = file.flatMap(line => line.split(" ")).count()
 //                    .map(word => (word, 1))
 //                    .reduceByKey(_ + _)
    println(counts)
 //   counts.saveAsTextFile("hdfs://192.168.1.12:9000/test-%s".format(System.currentTimeMillis()))
  }
}