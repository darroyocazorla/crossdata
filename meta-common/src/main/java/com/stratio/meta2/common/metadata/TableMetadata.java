/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.meta2.common.metadata;

import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;

import java.util.Map;

public class TableMetadata implements IMetadata {
  private final TableName name;

  private final Map<String, Object> options;

  private final Map<ColumnName, ColumnMetadata> columns;

  private final ClusterName clusterRef;

  public TableMetadata(TableName name, Map<String, Object> options,
      Map<ColumnName, ColumnMetadata> columns, ClusterName clusterRef) {
    this.name = name;
    this.options = options;
    this.columns = columns;
    this.clusterRef = clusterRef;

  }


  public TableName getName() {
    return name;
  }


  public Map<String, Object> getOptions() {
    return options;
  }

  public Map<ColumnName, ColumnMetadata> getColumns() {
    return columns;
  }

  public ClusterName getClusterRef() {
    return clusterRef;
  }


}
