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

package com.stratio.crossdata.streaming.actors

import akka.actor.{ActorSystem, Props}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.stratio.crossdata.streaming.actors.EphemeralQueryActor._
import com.stratio.crossdata.streaming.test.CommonValues
import org.apache.curator.test.TestingServer
import org.apache.curator.utils.CloseableUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpecLike, BeforeAndAfterAll, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class EphemeralQueryActorIT(_system: ActorSystem) extends TestKit(_system)
with DefaultTimeout
with ImplicitSender
with WordSpecLike
with BeforeAndAfterAll {

  def this() = this(ActorSystem("EphemeralQueryActor"))

  var zkTestServer: TestingServer = _
  var zookeeperConnection: String = _

  override def beforeAll: Unit = {
    zkTestServer = new TestingServer()
    zkTestServer.start()
    zookeeperConnection = zkTestServer.getConnectString
  }

  override def afterAll: Unit = {
    CloseableUtils.closeQuietly(zkTestServer)
    zkTestServer.stop()
  }

  "EphemeralQueryActor" should {
    "set up with zookeeper configuration without any error" in {
      _system.actorOf(Props(new EphemeralQueryActor(Map("connectionString" -> zookeeperConnection))))
    }
  }

  "EphemeralQueryActor" must {

    "AddListener the first message" in new CommonValues {

      val ephemeralQueryActor =
        _system.actorOf(Props(new EphemeralQueryActor(Map("connectionString" -> zookeeperConnection))))

      ephemeralQueryActor ! EphemeralQueryActor.AddListener

      expectMsg(new ListenerResponse(true))
    }

    "AddListener the not be the first message" in new CommonValues {

      val ephemeralQueryActor =
        _system.actorOf(Props(new EphemeralQueryActor(Map("connectionString" -> zookeeperConnection))))

      ephemeralQueryActor ! EphemeralQueryActor.GetQueries

      expectNoMsg()
    }

    "GetQueries is the second message" in new CommonValues {

      val ephemeralQueryActor =
        _system.actorOf(Props(new EphemeralQueryActor(Map("connectionString" -> zookeeperConnection))))

      ephemeralQueryActor ! EphemeralQueryActor.AddListener
      expectMsg(new ListenerResponse(true))

      ephemeralQueryActor ! EphemeralQueryActor.GetQueries
      expectMsg(new EphemeralQueriesResponse(Seq()))
    }
  }
}