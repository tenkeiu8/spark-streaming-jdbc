package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class JDBCStreamSource(sqlContext: SQLContext,
                       providerName: String,
                       parameters: Map[String, String]) extends Source with Logging {

  import sqlContext.implicits._

  private val df = sqlContext.sparkSession.read.format("jdbc")
    .options(parameters).load

  private val offsetColumn = parameters("offsetColumn")

  override def schema: StructType = df.schema

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    val startOffset: Long = start match {
      case Some(offset) => offset.json().toLong + 1
      case None => Long.MinValue
    }

    val endOffset: Long = end.json().toLong

    val rdd = df.filter(col(offsetColumn).between(startOffset, endOffset))
      .rdd.map { case Row(cols@_*) => InternalRow(cols: _*) }

    logInfo(s"Offset: ${start.toString} to ${end.toString}")
    sqlContext.internalCreateDataFrame(rdd, schema, true)
  }

  override def getOffset: Option[Offset] = {
    val firstItem = df.orderBy(col(offsetColumn).desc).select(col(offsetColumn).cast("Long")).as[Long].head(1)
    if (firstItem.isEmpty) None else Some(LongOffset(firstItem.head))
  }

  override def stop(): Unit = {
    logWarning("Stop is not implemented!")
  }

}

class JDBCStreamSourceProvider extends StreamSourceProvider with DataSourceV2 with DataSourceRegister {

  override def shortName(): String = "jdbc-streaming"

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {

    val df = sqlContext.sparkSession.read.format("jdbc")
      .options(parameters).load

    (shortName(), df.schema)
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