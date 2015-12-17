/*
 * Modifications and adaptations - Copyright (C) 2015 Stratio (http://stratio.com)
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.crossdata.test

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.crossdata.XDContext
import org.apache.spark.sql.{SQLConf, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object TestXDContext {

  val defaultSparkContext: SparkContext = {
    val defaultSparkConf = new SparkConf().set("spark.sql.testkey", "true").set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
    val externalMaster = Try(ConfigFactory.load().getString("spark.master"))
    val (sparkMaster, sparkConf) = externalMaster.map { master =>
      (master, defaultSparkConf.setJars(Seq(s"${new java.io.File(".").getCanonicalPath}/../server/target/crossdata-server-1.1.0-SNAPSHOT-jar-with-dependencies.jar")))
    } getOrElse (("local[2]", defaultSparkConf))

    new SparkContext(sparkMaster, "test-xdcontext", sparkConf)
  }
}

import TestXDContext._
/**
 * A special [[SQLContext]] prepared for testing.
 */
private[sql] class TestXDContext(sc: SparkContext) extends XDContext(sc) { self =>

  def this() = this(defaultSparkContext)


  // Use fewer partitions to speed up testing
  protected[sql] override def createSession(): SQLSession = new this.SQLSession()


  protected[sql] class SQLSession extends super.SQLSession {
    protected[sql] override lazy val conf: SQLConf = new SQLConf {
      override def numShufflePartitions: Int = 3
    }
  }

}
