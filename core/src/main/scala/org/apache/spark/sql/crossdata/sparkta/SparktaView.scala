package org.apache.spark.sql.crossdata.sparkta

import org.apache.spark.sql.types.StructType

case class Cube(name: String, groupByExprs: Seq[ColumnIdentifier], aggregationsExprs: Seq[AggregationExpression])

// TODO Currently, the first output is chosen. Soon, we should plan in order to use an optimal output
case class SparktaView(cube: Cube, sparktaOutput: Seq[SparktaOutput]){

  require(sparktaOutput.nonEmpty, "At least one output is required")
  def simpleString: String = ???

  def cubeSchema: StructType =
    sparktaOutput.head.schema

  def schema: StructType = {
    val nameToField = cubeSchema.fields.map(f => f.name -> f).toMap
    // It filters out aggregate columns from the cube
    StructType(
      cube.groupByExprs.flatMap {
        case ColumnIdentifier(colName) => nameToField.get(colName)
      }
    )
  }

}

object OutputDatasource extends Enumeration{
  type OutputDatasource = Value
  val Cassandra = Value("cassandra")
  val MongoDB = Value("mongodb")
  val Elasticsearch = Value("elasticsearch")
  val Parquet = Value("parquet")
  val CSV = Value("com.databricks.spark.csv")
  val Solr = Value("solr") //Lucid-words datasource
  val JDBC = Value("jdbc")
  val JSON = Value("json")

}
import org.apache.spark.sql.crossdata.sparkta.OutputDatasource._
// TODO serialize schema to json or String
// TODO options => options should contain datasource specific info => how to manage it?
case class SparktaOutput(datasource: OutputDatasource, schema: StructType, options: Map[String, String] = Map.empty)