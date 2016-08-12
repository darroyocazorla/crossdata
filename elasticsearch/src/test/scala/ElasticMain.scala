import java.util.UUID

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.elasticsearch.common.settings.Settings

object ElasticMain extends App {

  val ElasticHost: String = "127.0.0.1"
  val ElasticRestPort = 9200
  val ElasticNativePort = 9300
  val SourceProvider = "com.stratio.crossdata.connector.elasticsearch"
  val ElasticClusterName: String = "esCluster"
  val Index = s"highschool${UUID.randomUUID.toString.replaceAll("-", "")}"
  val Type = s"students${UUID.randomUUID.toString.replaceAll("-", "")}"


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
}
