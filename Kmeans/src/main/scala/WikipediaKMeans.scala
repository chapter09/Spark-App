/**
 * Created by wanghao on 6/12/14.
 */


package ca.haow.www

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans

import org.apache.spark.SparkContext._
import org.apache.spark.util.Vector
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.Random
import scala.io.Source


object WikipediaKMeans {
  def parseVector(line: String): Vector = {
    return new Vector(line.split(',').map(_.toDouble))
  }

  def closestPoint(p: Vector, centers: Array[Vector]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = p.squaredDist(centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    return bestIndex
  }

  def average(ps: Seq[Vector]): Vector = {
    val numVectors = ps.size
    var out = new Vector(ps(0).elements)
    for (i <- 1 until numVectors) {
      out += ps(i)
    }
    out / numVectors
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WikipediaKMeans")
    conf.setMaster("spark://sing12:7077")
    val sc = new SparkContext(conf)

    sc.addJar("/home/wanghao/Documents/Scala/Kmeans/target/scala-2.10/simple-project_2.10-1.0.jar")
    val K = 1000
    val convergeDist = 1e-6
    val data = sc.textFile(
      "hdfs://192.168.1.12:9000/wikipedia_featured").map(
        t => (t.split("#")(0), parseVector(t.split("#")(1)))).cache()
    val count = data.count()
    println("Number of records " + count)


    var centroids = data.takeSample(false, K, 42).map(x => x._2)
    var tempDist = 1.0
    do {
      var closest = data.map(p => (closestPoint(p._2, centroids), p._2))
      var pointsGroup = closest.groupByKey()
      var newCentroids = pointsGroup.mapValues(ps => average(ps.toSeq)).collectAsMap()
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += centroids(i).squaredDist(newCentroids(i))
      }
      for (newP <- newCentroids) {
        centroids(newP._1) = newP._2
      }
      println("Finished iteration (delta = " + tempDist + ")")
    } while (tempDist > convergeDist)
    println("Clusters with some articles:")
    val numArticles = 10
    for ((centroid, centroidI) <- centroids.zipWithIndex) {
      // print numArticles articles which are assigned to this centroid’s cluster
      data.filter(p => (closestPoint(p._2, centroids) == centroidI)).take(numArticles).foreach(
        x => println(x._1))
      println()
    }
    sc.stop()
    System.exit(0)

  }
}
