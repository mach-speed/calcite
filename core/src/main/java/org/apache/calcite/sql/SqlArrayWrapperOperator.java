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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Returns the wrapped expression.
 */
public class SqlArrayWrapperOperator extends SqlOperator {
  public SqlArrayWrapperOperator() {
    super("ARRAY_WRAPPER", SqlKind.ARRAY_WRAPPER, 95, true, null, null, null);
  }

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlArrayWrapper wrapper = (SqlArrayWrapper) call;
    wrapper.input.unparse(writer, leftPrec, rightPrec);
    writer.keyword("ARRAY_WRAPPER");
  }

  @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos,
      @Nullable SqlNode... operands) {
    return super.createCall(functionQualifier, pos, operands);
  }
}
