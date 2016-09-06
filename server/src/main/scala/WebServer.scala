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
import java.io.InputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLParameters, TrustManagerFactory}

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpsConnectionContext}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.TLSClientAuth
import com.typesafe.sslconfig.akka.AkkaSSLConfig

import scala.collection.immutable
import scala.io.StdIn

object WebServer {
  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher



    def xdhttps(
               sslContext:          SSLContext,
               sslConfig:           Option[AkkaSSLConfig]         = None,
               enabledCipherSuites: Option[immutable.Seq[String]]  = None,
               enabledProtocols:    Option[immutable.Seq[String]] = None,
               clientAuth:          Option[TLSClientAuth]        = None,
               sslParameters:       Option[SSLParameters]        = None) =
      new HttpsConnectionContext(sslContext, sslConfig, enabledCipherSuites, enabledProtocols, clientAuth, sslParameters)

    // TODO use akka ssl-config instead of manual => val sslConfig = AkkaSSLConfig()

    // MANUAL
    val password: Array[Char] = Array('s') // do not store passwords in code, read them from somewhere safe!

    val ks: KeyStore = KeyStore.getInstance("PKCS12")
    val keystore: InputStream = getClass.getClassLoader.getResourceAsStream("server.p12")

    require(keystore != null, "Keystore required!")
    ks.load(keystore, password)

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)

    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom())



    val httpsConnectionContext: HttpsConnectionContext = xdhttps(sslContext)



    val route =
      path("hello") {
        get {
          complete("say hello to akka http")
        }
      }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080, connectionContext = httpsConnectionContext )

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
