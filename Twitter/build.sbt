name := "Twitter"

version := "1.0"

scalaVersion := "2.10.4"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.3.1",
	"org.apache.spark" % "spark-streaming_2.10" % "1.3.1",
	"org.apache.spark" % "spark-streaming-twitter_2.10" % "1.3.1",
	"org.twitter4j" % "twitter4j-stream" % "3.0.3"
)