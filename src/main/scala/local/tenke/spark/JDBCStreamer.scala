package local.tenke.spark

import org.apache.spark.sql.SparkSession

object JDBCStreamer extends App {
  override def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("JDBC Stream Example")
      .master("local[*]")
      .getOrCreate()

    val opts = Map(
      //"url" -> "jdbc:postgresql:postgres",
      "url" -> "jdbc:postgresql:postgres",
      "dbtable" -> "test",
      "user" -> "postgres",
      "password" -> "spark"
    )

    val stream = spark
      .readStream
      .format("org.apache.spark.sql.jdbcstream.JDBCStreamSourceProvider")
      .options(opts)
      .load

    val out = stream.writeStream
      .outputMode("append")
      .format("console")
      .start()

    out.awaitTermination()

  }

}
