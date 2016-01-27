package org.apache.spark.sql.crossdata.sparkta

import java.nio.file.Paths

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.crossdata.test.SharedXDContextTest
import org.apache.spark.sql.execution.datasources.CreateTempTableUsing
import org.apache.spark.sql.types._

class SparktaIT extends SharedXDContextTest{

  val TableName = "cubename"
  val CubeExpansion = "_row"//cube, view
  val Dimension1 = ColumnIdentifier("dim1")
  val Dimension2 = ColumnIdentifier("dim2")
  val ExpressionAlias = "total_count"
  val Provider = OutputDatasource.JSON
  val OutputOptions: Map[String,String] = Map("path" -> Paths.get(getClass.getResource("/core-reference.conf").toURI()).toString)

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
    CreateTempTableUsing(TableIdentifier(TableName+CubeExpansion, None), Some(outputSchema), Provider.toString, OutputOptions).run(_xdContext)
  }
}
