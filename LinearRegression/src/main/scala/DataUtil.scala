import org.apache.spark._
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint

object DataUtil {
  def load(sc: SparkContext, file: String): MLDataSet = {
    val content = sc.textFile(file)

    val attributeContent = content.filter(l => l.startsWith("@attribute")).collect()
    val inputContent = content.filter(l => l.startsWith("@inputs")).collect()
    val outputContent = content.filter(l => l.startsWith("@outputs")).collect()
    val dataContent = content.filter(l => !l.startsWith("@")).collect()

    // Load attributes
    var attributes = attributeContent.foldLeft(scala.collection.mutable.MutableList[Attribute]()) {
      (l, s) => parseAttribute(l, s)
    }

    // Load inputs
    var inputs = inputContent.foldLeft(scala.collection.mutable.MutableList[String]()) {
      (l, s) => l ++= s.substring("@inputs ".length).split(", "); l;
    }

    // Load outputs
    var outputs = outputContent.foldLeft(scala.collection.mutable.MutableList[String]()) {
      (l, s) => l ++= s.substring("@outputs ".length).split(", "); l;
    }

    // Load data
    var data = dataContent.map(d => d.split(",").map(v => v.toDouble))

    return new MLDataSet(attributes, inputs, outputs, data)
  }

  def parseAttribute(l: scala.collection.mutable.MutableList[Attribute], s: String): scala.collection.mutable.MutableList[Attribute] = {
    var startBracket = ""
    var endBracket = ""

    // Determine if we are parsing a set of values or a range
    if (s.contains("{")) {
      startBracket = "{"
      endBracket = "}"
    } else if (s.contains("[")) {
      startBracket = "["
      endBracket = "]"
    } else {
      // No range found? Skip.
      return l
    }

    // Add the attribute to the list
    l += new Attribute(
      // Get the name of the attribute
      s.substring("@attribute ".length, s.indexOf(startBracket)),
      // Get the associated set/range
      s.substring(s.indexOf(startBracket) + 1, s.indexOf(endBracket)).split(",").map(v => v.toDouble))

    return l
  }
}

class MLDataSet(
    _attributes: scala.collection.mutable.MutableList[Attribute],
    _inputs: scala.collection.mutable.MutableList[String],
    _outputs: scala.collection.mutable.MutableList[String],
    _data: Array[Array[Double]]) {

  var attributes: scala.collection.mutable.MutableList[Attribute] = _attributes
  var inputs: scala.collection.mutable.MutableList[String] = _inputs
  var outputs: scala.collection.mutable.MutableList[String] = _outputs
  var data: Array[Array[Double]] = _data

  def asLabeledPoints(): Seq[LabeledPoint] = {
    data.map(d => normalisedLabeledPoint(d))
  }
  
  private def normalisedLabeledPoint(values: Array[Double]): LabeledPoint = {
    var vn = normaliseValues(values)
    
    // Create a LabeledPoint using the last value as the output and remaining as the inputs 
    return LabeledPoint(vn(vn.length - 1), Vectors.dense(vn.take(vn.length - 1)))
  }

  private def normaliseValues(values: Array[Double]): Array[Double] = {
    var v = values.clone()

    for (i <- 0 to v.length - 1) {
      val min = this.attributes(i).min
      val max = this.attributes(i).max

      // Normalise the value between the min and max values of the attribute
      v(i) = (v(i) - min) / (max - min)
    }

    return v
  }
}

class Attribute(_name: String, _values: Array[Double]) {
  var name: String = _name

  var values: Array[Double] = _values

  var min: Double = _values.min
  var max: Double = _values.max
}