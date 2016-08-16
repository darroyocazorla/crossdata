/*
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
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.apache.spark.sql.crossdata.ExecutionType
import org.apache.spark.sql.crossdata.test.SharedXDContextTest
import org.elasticsearch.common.settings.Settings
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ElasticMain extends SharedXDContextTest{

  val ElasticHost: String = "127.0.0.1"
  val ElasticRestPort = 9200
  val ElasticNativePort = 9300
  val SourceProvider = "com.stratio.crossdata.connector.elasticsearch"
  val ElasticClusterName: String = "esCluster"

  val ElasticResource: String = "twitt/tweet"

  val settings = Settings.settingsBuilder()
    .put("cluster.name", ElasticClusterName)
    .put("shield.user", "crossdata:stratio")
    .put("plugin.types", "org.elasticsearch.shield.ShieldPlugin")
    .build()
  val uri = ElasticsearchClientUri(s"elasticsearch://$ElasticHost:$ElasticNativePort")
  val elasticClient = ElasticClient.transport(settings, uri)

  val result = elasticClient.execute {
    val a = search in "twitt" / "tweet"
    // a._builder.putHeader("es-shield-runas-user","jacknich")
    a
  } await

  println (result)

  "Elasticsearch" should "dsjfojds" in {
    
    val myTable = "viewName"

    val sentence =
      s"""|CREATE TEMPORARY TABLE $myTable USING com.stratio.crossdata.connector.elasticsearch OPTIONS (es.resource '$ElasticResource',
          | es.nodes '$ElasticHost',
          | es.port '$ElasticRestPort',
          | es.nativePort '$ElasticNativePort',
          | es.cluster '$ElasticClusterName',
          | es.nodes.wan.only 'true',
          | es.net.http.auth.user 'crossdata',
          | es.net.http.auth.pass 'stratio'
          |)""".stripMargin
    sql(sentence).collect()

    sql(s"SELECT * FROM $myTable").collect(ExecutionType.Spark).foreach(println)
  }


}
