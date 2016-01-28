/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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