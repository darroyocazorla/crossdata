package org.apache.spark.sql.crossdata.sparkta

sealed trait AggregationExpression

object OperationType extends Enumeration{
  type OperationType = Value
  val Count, CountDistinct, Sum, Avg, Min, Max = Value
}

import OperationType._

case class ColumnOperation(alias: String, field: ColumnIdentifier, opType: OperationType) extends AggregationExpression

case class GlobalCountOperation(alias: String, isDistinct: Boolean = false) extends AggregationExpression

// TODO According to Sparkta doc, the colIdentifier can have name and field, so an alias should be added
case class ColumnIdentifier(field: String)