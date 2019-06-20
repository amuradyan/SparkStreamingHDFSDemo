package example

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ScalaSparkStreamingHDFSDemo {

  case class Entity(tt: Int, vv: String)

  val EntitySchema = new StructType()
    .add("entities", ArrayType(
      new StructType()
        .add("tt", IntegerType)
        .add("vv", StringType)
    )
    )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Scala Spark Demo")
      .getOrCreate()

    import spark.implicits._

    val inputDirectory = "hdfs://localhost:9000/scala-demo/input"
    //    val inputDirectory = "file:///home/spectrum/playground/tmp"
    val rawEntities = spark.readStream
      .schema(EntitySchema)
      .option("multiline", "true")
      .json(inputDirectory)

    val entities = rawEntities
      .select(explode($"entities") as "entity")
      .select($"entity.tt", $"entity.vv")

    val ds1 = entities.as[Entity].filter(_.tt == 1)
    val ds2 = entities.as[Entity].filter(_.tt == 2)
    val ds3 = entities.as[Entity].filter(_.tt == 3)

    val d1stream = ds1.writeStream
//      .format("console")
            .outputMode("append")
            .format("json")
            .option("checkpointLocation", "hdfs://localhost:9000/scala-demo/output/t1")
            .option("path", "hdfs://localhost:9000/scala-demo/output/t1")
      .start()

    val d2stream = ds2.writeStream
//      .format("console")
            .outputMode("append")
            .format("json")
            .option("checkpointLocation", "hdfs://localhost:9000/scala-demo/output/t2")
            .option("path", "hdfs://localhost:9000/scala-demo/output/t2")
      .start()

    val d3stream = ds3.writeStream
//      .format("console")
            .outputMode("append")
            .format("json")
            .option("checkpointLocation", "hdfs://localhost:9000/scala-demo/output/t3")
            .option("path", "hdfs://localhost:9000/scala-demo/output/t3")
      .start()

    d1stream.awaitTermination()
    d2stream.awaitTermination()
    d3stream.awaitTermination()
  }
}
