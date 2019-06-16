package example

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object Main {
  def main(args: Array[String]): Unit = {
    val dataDirectory = "hdfs://my-dir"
    val conf = new SparkConf().setAppName("ScalaSpark").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val data = ssc.textFileStream(dataDirectory)
    // Read the JSON via spark streaming (this will convert to DF)
    // For each DF 
  }
}