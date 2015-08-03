import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object TwitterPopular {

  def run(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TwitterPopular")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    
    val stream = TwitterUtils.createStream(ssc, None, args)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topics = hashTags.map((_, 1))
                 .reduceByKeyAndWindow(_ + _, Seconds(10))
                 .map{case (topic, count) => (count, topic)}
                 .transform(_.sortByKey(false))

    topics.foreachRDD(rdd => {
      val top = rdd.take(10)
      println("\nTop topics (%s total):".format(rdd.count()))
      top.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
