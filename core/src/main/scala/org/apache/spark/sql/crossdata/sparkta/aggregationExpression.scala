package org.apache.spark.sql.crossdata.sparkta

sealed trait AggregationExpression

object OperationType extends Enumeration{
  type OperationType = Value
  val Count, CountDistinct, Sum, Avg, Min, Max = Value
}

import OperationType._

case class ColumnOperation(alias: String, field: ColumnIdentifier, opType: OperationType) extends AggregationExpression

case class GlobalCountOperation(alias: String, isDistinct: Boolean = false) extends AggregationExpression

case class ColumnIdentifier(field: String)