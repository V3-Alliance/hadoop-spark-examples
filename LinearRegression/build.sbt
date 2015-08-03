name := "LinearRegression"

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
	"org.apache.spark" %% "spark-mllib" % "1.3.1",
	"com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
	"org.apache.hadoop" % "hadoop-hdfs" % "2.7.1"
)