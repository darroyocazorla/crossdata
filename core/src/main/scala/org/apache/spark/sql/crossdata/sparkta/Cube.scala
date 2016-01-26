package org.apache.spark.sql.crossdata.sparkta

import org.apache.spark.sql.types.StructType

case class Cube(name: String, groupByExprs: Seq[ColumnIdentifier], aggregationsExprs: Seq[AggregationExpression])

case class SparktaView(cube: Cube, output: Seq[SparktaOutput])

object OutputDatasource extends Enumeration{
  type OutputDatasource = Value
  val Cassandra = Value("cassandra")
  val MongoDB = Value("mongodb")
  val Elasticsearch = Value("elasticsearch")
  val Parquet = Value("parquet")
  val CSV = Value("com.databricks.spark.csv")
  val Solr = Value("solr") //Lucid-words datasource

}
import OutputDatasource._
case class SparktaOutput(datasource: OutputDatasource, schema: StructType, options: Map[String, String])

