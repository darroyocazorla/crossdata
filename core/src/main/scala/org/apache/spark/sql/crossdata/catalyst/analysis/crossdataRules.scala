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
package org.apache.spark.sql.crossdata.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.crossdata.sparkta
import org.apache.spark.sql.crossdata.sparkta.GlobalCountOperation
import org.apache.spark.sql.crossdata.sparkta.SparktaRelation
import org.apache.spark.sql.crossdata.sparkta.SparktaView
import org.apache.spark.sql.execution.datasources.LogicalRelation

// SELECT sth as alias GROUP BY alias is allowed thanks to the rule
object ResolveAggregateAlias extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case p: LogicalPlan if !p.childrenResolved => p

    case a@Aggregate(grouping, aggregateExp, child) if child.resolved && !a.resolved && groupingExpressionsContainAlias(grouping, aggregateExp) =>
      val newGrouping = grouping.map { groupExpression =>
        groupExpression transformUp {
          case u@UnresolvedAttribute(Seq(aliasCandidate)) =>
            aggregateExp.collectFirst {
              case Alias(resolvedAttribute, aliasName) if aliasName == aliasCandidate =>
                resolvedAttribute
            }.getOrElse(u)
        }
      }
      a.copy(groupingExpressions = newGrouping)

  }

  private def groupingExpressionsContainAlias(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression]): Boolean = {
    def aggregateExpressionsContainAliasReference(aliasCandidate: String) =
      aggregateExpressions.exists {
        case Alias(resolvedAttribute, aliasName) if aliasName == aliasCandidate =>
          true
        case _ =>
          false
      }

    groupingExpressions.exists {
      case u@UnresolvedAttribute(Seq(aliasCandidate)) =>
        aggregateExpressionsContainAliasReference(aliasCandidate)
      case _ => false
    }
  }

  // Catalyst config cannot be read, so the most restrictive resolver is used
  val resolver: Resolver = caseSensitiveResolution

}


object ResolveSparktaRelation extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    var sparktaView: Option[SparktaView] = None

    val pWithSR = plan resolveOperators {
        case p: LogicalPlan if !p.childrenResolved => p
        case SparktaRelation(sqlContext, spView) =>
          // TODO LogicalRelation within the sparktaView ?? vs handle databases
          sparktaView = Some(spView)
          sqlContext.catalog.lookupRelation(Seq(spView.cube.name + sparkta.CubeExtendedName))
      }

    if (sparktaView.isEmpty) {
      pWithSR
    } else {
      val pWithAR = pWithSR resolveOperators {
        case p if p.children.nonEmpty && p.missingInput.nonEmpty =>
          // val missingAttributes = p.missingInput.mkString(",")
          val sparktaAttributes = p.collectFirst { case lRelation: LogicalRelation => lRelation.output }.getOrElse(Seq.empty)
          // TODO do several checks appart from the name
          val sparktaAttributeMap = sparktaAttributes.map(att => att.name -> att).toMap

          // TODO use p.inputSet instead?
          p transformAllExpressions {
            // TODO can we take advantage of metadata?
            case attReference: AttributeReference if sparktaAttributeMap.contains(attReference.name) =>
              sparktaAttributeMap(attReference.name)
          }
      }

      val pWithSub = pWithAR transform {
        case Subquery(_, Subquery(_, child)) => child
        case Subquery(_, child) => child
      }

      pWithSub resolveOperators {
        // TODO add a lot of if clauses (with filters, with operators, etc...)
        case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
          val (operators, sparktaDimensions) = aggregateExpressions partition {
            case _: AttributeReference => false
            case Alias(_:AttributeReference,_) => false
            case _ => true
          }
          val newAggExpr: Seq[NamedExpression] = operators.map{
            case nalias @ Alias(Count(Literal(1,_)), aliasName) =>
              val globalCountOpt = sparktaView.get.cube.aggregationsExprs.collectFirst{
                case g@GlobalCountOperation(alias, false) =>g
              }
              // TODO improve log and exception
              globalCountOpt.map{ globCount =>
                val ali = globCount.alias
                // TODO improve copy&paste
                val sparktaAttributes = pWithSub.collectFirst { case lRelation: LogicalRelation => lRelation.output }.getOrElse(Seq.empty)
                val sparktaAttributeMap = sparktaAttributes.map(att => att.name -> att).toMap
                val attributeFromSparkta = sparktaAttributeMap.getOrElse(ali, throw new AnalysisException(s"Column not found $ali"))
                nalias.withNewChildren(Seq(Sum(attributeFromSparkta))).asInstanceOf[Alias]
              }.getOrElse[NamedExpression](throw new AnalysisException("Aggregation not supported"))

            case other => other
          }
          a.copy(aggregateExpressions = sparktaDimensions ++ newAggExpr)
          // TODO the groupby should incude all the cube fields => add validation
          // TODO operator should match as well
      }
    }

  }
}







/*
object ResolveSparktaRelation extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case p: LogicalPlan if !p.childrenResolved => p
    case SparktaRelation(sqlContext, sparktaView) =>
      // TODO LogicalRelation within the sparktaView ?? vs handle databases
      sqlContext.catalog.lookupRelation(Seq(sparktaView.cube.name + CubeExtendedName))
  }

}


object ReplaceAttributeIDs extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case p if p.children.nonEmpty && p.missingInput.nonEmpty =>
      // val missingAttributes = p.missingInput.mkString(",")
      val sparktaAttributes = p.collectFirst { case lRelation: LogicalRelation => lRelation.output }.getOrElse(Seq.empty)
      // TODO do several checks appart from the anem
      val sparktaAttributeMap = sparktaAttributes.map(att => att.name -> att).toMap

      // TODO use p.inputSet instead?
      p transformAllExpressions {
        // TODO can we take advantage of metadata?
        case attReference: AttributeReference if sparktaAttributeMap.contains(attReference.name) =>
          sparktaAttributeMap(attReference.name)
      }

    case _ =>
      plan
  }
}


object EliminateSparktaSubQueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, Subquery(_, child)) => child
    case Subquery(_, child) => child
  }
}



object ReplaceSparktaCountAll extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // TODO add if
    case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
      val operators = aggregateExpressions.filter {
        case _: AttributeReference => false
        case Alias(_:AttributeReference,_) => false
        case _ => true
      }

      // TODO the groupby should incude all the cube fields => add validation
      ???

      case _ =>
        plan
    }

}

*/
