package org.apache.spark.sql.jdbcstream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartitioningInfo, JDBCRDD, JDBCRelation}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Source, StreamExecution}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}

class JDBCStreamSource(sqlContext: SQLContext,
                       providerName: String,
                       parameters: Map[String, String]) extends Source {

  val df = sqlContext.sparkSession.read.format("jdbc")
    .options(parameters).load

  override def schema: StructType = df.schema

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    import sqlContext.implicits._
    println("hooray!")
    /*
        val rdd = sqlContext.sparkContext.parallelize(1 to 100).map{ x => (x.toLong,x.toLong)}
            .map { case (x,y) => InternalRow(x,y)}
        currentOffset += 10
         sqlContext.internalCreateDataFrame(
          rdd, schema, isStreaming = true)

        val options = new JDBCOptions(parameters)

        val sc = sqlContext.sparkContext
        val partitioningInfo = JDBCPartitioningInfo(options.partitionColumn.get, options.lowerBound.get, options.upperBound.get, options.numPartitions.get)
        val columnPartition = JDBCRelation.columnPartition(partitioningInfo)
        val rows: RDD[InternalRow] = JDBCRDD.scanTable(sc,schema, Array(), Array(), columnPartition, options)
        JDBCRelation(columnPartition, options)(sqlContext.sparkSession).buildScan(Array(), Array()).asInstanceOf[RDD[InternalRow]]
        */

    /*
    val rdd = sqlContext.sparkSession.read.format("jdbc")
      .options(Map("driver" -> driverClass,
        "url" -> jdbcUrl, "dbtable" -> dbTable,
        "partitionColumn" -> partitionColumn, "numPartitions" -> numPartitions, "lowerBound" -> lowerBound, "upperBound" -> upperBound)).load
      .rdd
      .asInstanceOf[RDD[InternalRow]]
      */

    val df = sqlContext.sparkSession.read.format("jdbc")
      .options(parameters).load

    val rdd = df.rdd.map { case Row(cols@_*) => InternalRow(cols: _*) }
    //.asInstanceOf[RDD[InternalRow]]

    currentOffset += 10

    sqlContext.internalCreateDataFrame(rdd, schema, true)
  }

  @volatile private var currentOffset = 0

  override def getOffset: Option[Offset] = Some(new StreamJDBCOffset(currentOffset))

  override def stop(): Unit = Unit

}

class StreamJDBCOffset(val offset: Int) extends Offset {
  override def json(): String = s"""{ "offset": $offset}"""
}

class JDBCStreamSourceProvider extends StreamSourceProvider with DataSourceV2 {
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {

    val df = sqlContext.sparkSession.read.format("jdbc")
      .options(parameters).load

    ("JDBC-schema", df.schema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {

    new JDBCStreamSource(sqlContext, providerName, parameters)
  }
}