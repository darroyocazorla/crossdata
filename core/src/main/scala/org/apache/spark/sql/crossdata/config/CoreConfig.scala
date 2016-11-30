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
package org.apache.spark.sql.crossdata.config

import java.io.File

import com.stratio.common.utils.components.logger.impl.SparkLoggerComponent
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.internal.SQLConf

import scala.util.Try


object CoreConfig {

  val DefaultCatalogIdentifier = "defaultcat"

  val CoreBasicConfig = "core-reference.conf"
  val ParentConfigName = "crossdata-core"
  val CoreUserConfigFile = "external.config.filename"
  val CoreUserConfigResource = "external.config.resource"
  val CatalogConfigKey = "catalog"
  val LauncherKey= "launcher"
  val JarsRepo = "jars"
  val HdfsKey = "hdfs"

  val DerbyClass = "org.apache.spark.sql.crossdata.catalog.persistent.DerbyCatalog"
  val DefaultSecurityManager = "com.stratio.crossdata.security.DummyCrossdataSecurityManager"
  val ZookeeperClass = "org.apache.spark.sql.crossdata.catalog.persistent.ZookeeperCatalog"
  val ZookeeperStreamingClass = "org.apache.spark.sql.crossdata.catalog.streaming.ZookeeperStreamingCatalog"
  val StreamingConfigKey = "streaming"
  val SecurityConfigKey = "security"
  val SecurityManagerConfigKey = "manager"
  val ClassConfigKey = "class"
  val PrefixKey = "prefix"

  val AuditConfigKey = "audit"
  val UserConfigKey = "user"
  val PasswordConfigKey = "password"
  val SessionConfigKey = "session"
  val CatalogClassConfigKey = s"$CatalogConfigKey.$ClassConfigKey"
  val CatalogPrefixConfigKey = s"$CatalogConfigKey.$PrefixKey" // TODO rename prefix to catalogIdentifier
  val StreamingCatalogClassConfigKey = s"$StreamingConfigKey.$CatalogConfigKey.$ClassConfigKey"

  val SecurityEnabledKey = s"$SecurityConfigKey.$SecurityManagerConfigKey.enabled"
  val SecurityClassConfigKey = s"$SecurityConfigKey.$SecurityManagerConfigKey.$ClassConfigKey"

  val SparkSqlConfigPrefix = "config.spark.sql" //WARNING!! XDServer is using this path to read its parameters


  // WARNING: It only detects paths starting with "config.spark.sql"
  def configToSparkSQL(config: Config, defaultSqlConf: SQLConf = new SQLConf): SQLConf = {

    import scala.collection.JavaConversions._

    val sparkSQLProps: Map[String,String] =
      config.entrySet()
        .map(e => (e.getKey, e.getValue.unwrapped().toString))
        .toMap
        .filterKeys(_.startsWith(CoreConfig.SparkSqlConfigPrefix))
        .map(e => (e._1.replace("config.", ""), e._2))


    def sqlPropsToSQLConf(sparkSQLProps: Map[String, String], sqlConf: SQLConf): SQLConf = {
      sparkSQLProps.foreach { case (key, value) =>
        sqlConf.setConfString(key, value)
      }
      sqlConf
    }

    sqlPropsToSQLConf(sparkSQLProps, defaultSqlConf)
  }
}

trait CoreConfig extends SparkLoggerComponent {

  import CoreConfig._

  val logger: Logger

  val config: Config = {

    var defaultConfig = ConfigFactory.load(CoreBasicConfig).getConfig(ParentConfigName)
    val envConfigFile = Option(System.getProperties.getProperty(CoreUserConfigFile))
    val configFile = envConfigFile.getOrElse(defaultConfig.getString(CoreUserConfigFile))
    val configResource = defaultConfig.getString(CoreUserConfigResource)

    if (configResource != "") {
      val resource = getClass.getClassLoader.getResource(configResource)
      if (resource != null) {
        val userConfig = ConfigFactory.parseResources(configResource).getConfig(ParentConfigName)
        defaultConfig = userConfig.withFallback(defaultConfig)
        logInfo("User resource (" + configResource + ") found in resources")
      } else {
        logWarning("User resource (" + configResource + ") hasn't been found")
        val file = new File(configResource)
        if (file.exists()) {
          val userConfig = ConfigFactory.parseFile(file).getConfig(ParentConfigName)
          defaultConfig = userConfig.withFallback(defaultConfig)
          logInfo("User resource (" + configResource + ") found in classpath")
        } else {
          logWarning("User file (" + configResource + ") hasn't been found in classpath")
        }
      }
    }

    if (configFile != "") {
      val file = new File(configFile)
      if (file.exists()) {
        val parsedConfig = ConfigFactory.parseFile(file)
        if(parsedConfig.hasPath(ParentConfigName)){
          val userConfig = ConfigFactory.parseFile(file).getConfig(ParentConfigName)
          defaultConfig = userConfig.withFallback(defaultConfig)
          logInfo("External file (" + configFile + ") found")
        } else {
          logger.info(s"External file ($configFile) found but not configuration found under $ParentConfigName")
        }

      } else {
        logWarning("External file (" + configFile + ") hasn't been found")
      }
    }

    // System properties
    val systemPropertiesConfig =
      Try(
        ConfigFactory.parseProperties(System.getProperties).getConfig(ParentConfigName)
      ).getOrElse(
        ConfigFactory.parseProperties(System.getProperties)
      )

    defaultConfig = systemPropertiesConfig.withFallback(defaultConfig)

    ConfigFactory.load(defaultConfig)
  }
}


