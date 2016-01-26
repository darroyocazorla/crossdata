package org.apache.spark.sql.crossdata.sparkta

import org.apache.spark.sql.types.StructType

case class Cube(name: String, groupByExprs: Seq[ColumnIdentifier], aggregationsExprs: Seq[AggregationExpression])

case class SparktaView(cube: Cube, output: Seq[SparktaOutput])

case class SparktaOutput(source: String, schema: StructType, options: Map[String, String])