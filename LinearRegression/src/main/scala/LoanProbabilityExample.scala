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

object LoanProbabilityExample {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("LoanProbabilityExample")
    val sc = new SparkContext(sparkConf)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val ratingTypes = Array("A", "B", "C")

    // Generate some mock data
    var people = new ListBuffer[Person]()

    for (p <- 1 to 10) {
      val rating = ratingTypes(Random.nextInt(ratingTypes.length))
      val salary = Random.nextInt(100) * 1000
      val age = Random.nextInt(90) + 18
      
      people += Person(rating, salary, age)
    }

    // Convert the people data to vector format
    var features = generateFeatures(people)
    
    // Assign the vector data with the load probability label
    var featuresWithLabels = generateFeaturesWithLabels(features)

    val data = sc.parallelize(featuresWithLabels)

    // Split data into two sets for training and testing
    val splits = data.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache
    val testing = splits(1).cache

    val numTraining = training.count()
    val numTesting = testing.count()

    // Run the analysis
    val algorithm = new LinearRegressionWithSGD()
    val model = algorithm.run(training)
    val prediction = model.predict(testing.map(_.features))
    val predictionAndLabel = prediction.zip(testing.map(_.label))

    // Display the predictions against their actual values
    println(s"\nTraining: $numTraining, Testing: $numTesting\n")
    predictionAndLabel.foreach((result) => println(s"Predicted: ${result._1}, Actual: ${result._2}"))

    // Calculate the Root Mean Square Error 
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)

    val rmse = math.sqrt(loss / numTesting)

    println(s"\nRMSE = $rmse\n")
    
    // Persist the output by merging all values into the one file
    val outputDirectory = "/tmp/linearRegression"
    val outputFile = "/tmp/linearRegression.csv"
    
    fs.delete(new Path(outputDirectory), true)
    fs.delete(new Path(outputFile), false)
    
    val output = predictionAndLabel.map(result => result._1 + "," + result._2)
    output.saveAsTextFile(outputDirectory)
    
    FileUtil.merge(outputDirectory, outputFile)

    sc.stop()
  }

  def generateFeatures(people: Seq[Person]): Seq[Vector] = {
    val maxIncome = people.map(_.income).max
    val maxAge = people.map(_.age).max

    people.map(p =>
      Vectors.dense(
        if (p.rating == "A") 0.8 else if (p.rating == "B") 0.6 else 0.3,
        p.income / maxIncome,
        p.age.toDouble / maxAge))
  }

  def generateFeaturesWithLabels(features: Seq[Vector]): Seq[LabeledPoint] = {
    features.map(l => LabeledPoint(getLoanProbability(l), l))
  }

  def getLoanProbability(person: Vector): Double = {
    // Salary * (1.(age factor)) * Rating
    val p = (person(1) * (1 + (person(2).toFloat / 100f)) * person(0))
    
    return math.min(p, 1d)
  }
}

case class Person(rating: String, income: Double, age: Int)