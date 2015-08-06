import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer
import scala.util.Random

import java.io.File

object GenericExample {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("GenericExample")
    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    
    val ds = DataUtil.load(sc, args(0))
    
    val data = sc.parallelize(ds.asLabeledPoints())

    // Split data into two sets for training and testing
    val splits = data.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache
    val testing = splits(1).cache

    val numTraining = training.count()
    val numTesting = testing.count()

    // Run the analysis
    val model = LinearRegressionWithSGD.train(training, args(1).toInt)
    val prediction = model.predict(testing.map(_.features))
    val predictionAndLabel = prediction.zip(testing.map(_.label))

    // Display the predictions against their actual values
    println(s"\nTraining: $numTraining, Testing: $numTesting\n")
    predictionAndLabel.foreach((result) => println(s"Predicted: ${result._1},\tActual: ${result._2},\tDifference: ${result._2 - result._1}"))

    // Calculate the Root Mean Square Error 
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)

    val rmse = math.sqrt(loss / numTesting)

    println(s"\nRMSE = $rmse\n")

    sc.stop()
  }
}