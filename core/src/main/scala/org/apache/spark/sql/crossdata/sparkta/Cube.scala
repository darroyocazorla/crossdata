package org.apache.spark.sql.crossdata.sparkta

import org.apache.spark.sql.types.StructType

case class Cube(name: String, groupByExprs: Seq[ColumnIdentifier], aggregationsExprs: Seq[AggregationExpression])

case class SparktaView(cube: Cube, output: Seq[SparktaOutput])

object OutputSource extends Enumeration{
  type OutputSource = Value
  val Cassandra = Value("cassandra")
  val MongoDB = Value("mongodb")
  val Elasticsearch = Value("elasticsearch")
  val Parquet = Value("parquet")
  val CSV = Value("com.databricks.spark.csv")
  val Solr = Value("solr") //Lucid-words datasource

}
import OutputSource._
case class SparktaOutput(source: OutputSource, schema: StructType, options: Map[String, String])

