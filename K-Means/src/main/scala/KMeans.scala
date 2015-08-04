import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object KMeans {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(sparkConf)
    
    val data = sc.textFile(args(0))
    val training = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    
    val numClusters = args(1).toInt
    val numIterations = args(2).toInt
    
    val clusters = org.apache.spark.mllib.clustering.KMeans.train(training, numClusters, numIterations)
    
    println(s"\nClusters: ${clusters.k}\n")
    
    val prediction = clusters.predict(training)
    val predictionAndLabel = prediction.zip(training)
    
    predictionAndLabel.foreach((result) => println(s"Predicted Cluster: ${result._1}, Value: ${result._2}"))
    
    val wssse = clusters.computeCost(training)
    
    println(s"\nCost: $wssse\n")
    
    sc.stop()
  }
}