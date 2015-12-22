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
package org.apache.spark.sql.crossdata.test

import org.apache.spark.sql.crossdata.CrossdataVersion

trait CoreWithSharedContext extends SharedXDContextTest{
  override def jarPathList: Seq[String] =
    // TODO include snapshot within the version
    Seq(s"core/target/crossdata-core-$CrossdataVersion-SNAPSHOT-jar-with-dependencies.jar", s"core/target/crossdata-core-$CrossdataVersion-SNAPSHOT-tests.jar")

/*  /**
   * List of files required to execute ITs
   */
  override protected def filePathList: Seq[String] = Seq(Paths.get(getClass.getResource("/catalog-reference.conf").toURI()).toString)*/
}
