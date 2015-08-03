import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object TwitterSimple {

  def run(args: Array[String]) {
    val filters = args

    val sparkConf = new SparkConf().setAppName("TwitterSimple")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val statuses = stream.map(status => status.getText())
    statuses.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
