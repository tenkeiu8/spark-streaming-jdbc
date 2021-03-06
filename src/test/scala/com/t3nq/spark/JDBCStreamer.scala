package com.t3nq.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object JDBCStreamer extends App with Logging {
  override def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("JDBC Stream Example")
      .master("local[*]")
      .getOrCreate()

    val opts = Map(
      "url" -> "jdbc:postgresql://localhost:5432/postgres",
      "dbtable" -> "test",
      "user" -> "postgres",
      "password" -> "spark",
      "offsetColumn" -> "col_a"
    )

    val stream = spark
      .readStream
      .format("org.apache.spark.sql.execution.streaming.sources.JDBCStreamSourceProvider")
      .options(opts)
      .load

    val out = stream.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "target/checkopoint")
      .start()

    logInfo("Streaming job started...")

    out.awaitTermination()

  }

}
