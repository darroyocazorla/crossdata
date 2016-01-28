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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.Statistics

// TODO make constructor private => public constructor which receive zookeeper location where sparktaView can be found
// TODO register the LogicalPlan
private[sql] case class SparktaRelation(@transient sqlContext: SQLContext, sparktaView: SparktaView)
  extends LeafNode with MultiInstanceRelation{

  override def output: Seq[Attribute] =
    sparktaView.schema.toAttributes

  override def simpleString: String =
    sparktaView.simpleString

  override def statistics: Statistics =
    Statistics(sqlContext.conf.defaultSizeInBytes)

  override def newInstance(): this.type =
    SparktaRelation(sqlContext, sparktaView).asInstanceOf[this.type]
}


