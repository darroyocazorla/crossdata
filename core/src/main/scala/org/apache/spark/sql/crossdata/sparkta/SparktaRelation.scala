package org.apache.spark.sql.crossdata.sparkta

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.Statistics

// TODO make constructor private => public constructor which receive zookeeper location where sparktaView can be found
// TODO register the LogicalPlan
private[sql] case class SparktaRelation(sqlContext: SQLContext, sparktaView: SparktaView)
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


