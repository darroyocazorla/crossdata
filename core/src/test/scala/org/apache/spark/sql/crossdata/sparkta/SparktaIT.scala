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

import java.nio.file.Paths

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.crossdata.test.SharedXDContextTest
import org.apache.spark.sql.execution.datasources.CreateTempTableUsing
import org.apache.spark.sql.types._

class SparktaIT extends SharedXDContextTest{

  val TableName = "cubename"
  val RowTableName = TableName+CubeExtendedName
  val Dimension1 = ColumnIdentifier("dim1")
  val Dimension2 = ColumnIdentifier("dim2")
  val ExpressionAlias = "total_count"
  val Provider = OutputDatasource.JSON
  val OutputOptions: Map[String,String] = Map("path" -> Paths.get(getClass.getResource("/cubename_cube.json").toURI()).toString)

  it should ".." in {
    val cube = Cube(TableName, Seq(Dimension1, Dimension2), Seq(GlobalCountOperation(ExpressionAlias)))
    // TODO duplicate total_count (aggregationExpression and structType)
    val outputSchema =  StructType(Seq(StructField(Dimension1.field, StringType), StructField(Dimension2.field, StringType), StructField(ExpressionAlias, LongType)))
    val output = SparktaOutput(Provider, outputSchema)
    val sparktaView = SparktaView(cube, Seq(output))
    val sparktaRelation = new SparktaRelation(_xdContext, sparktaView)
    _xdContext.catalog.registerTable(Seq(TableName), sparktaRelation)
    // TODO the TableName_Row register should be hidden => automatically with the TableName regiser
    // TODO the easiest way should be to have a different executionPlan (instead of logical relation) => but it should avoided because of the complexity to maintain that code
    CreateTempTableUsing(TableIdentifier(RowTableName, None), Some(outputSchema), Provider.toString, OutputOptions).run(_xdContext)

    //_xdContext.sql(s"SELECT dim1, dim2, total_count FROM $RowTableName").show()
    //_xdContext.sql(s"SELECT dim1, dim2 FROM $TableName WHERE dim1 = 'Crossdata'").show()


    _xdContext.sql(s"SELECT dim1, dim2, count(*) FROM $TableName GROUP BY dim1, dim2").show()
    //_xdContext.sql(s"SELECT dim1, dim2, count(*) as alias FROM $TableName GROUP BY dim1, dim2").show()
  }
}
