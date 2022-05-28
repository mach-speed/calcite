/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.parser;

import org.apache.calcite.sql.fun.SqlLoopEqualsOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Ha3SqlOperatorTable is the operator table for Ha3SQL.
 *
 * <p>It is a subclass of {@link SqlStdOperatorTable} that adds the
 * {@link SqlLoopEqualsOperator} operator.
 */
public class Ha3SqlOperatorTable extends SqlStdOperatorTable {
  public static final SqlStdOperatorTable INSTANCE = new Ha3SqlOperatorTable();

  private Ha3SqlOperatorTable() {
    super();
  }

  public static final SqlLoopEqualsOperator LOOP_EQUALS = new SqlLoopEqualsOperator();
}
