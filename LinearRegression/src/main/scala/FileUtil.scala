import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object FileUtil {
  def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    
    org.apache.hadoop.fs.FileUtil.copyMerge(hdfs, new Path(srcPath),
                       hdfs, new Path(dstPath),
                       false, hadoopConfig, null)
  }
}