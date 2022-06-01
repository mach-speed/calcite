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
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * A wrapper for a list of {@link SqlNode}s.
 *
 * <p>This is used to represent a list of expressions in a {@link SqlCall}
 * (e.g. the operands of a {@link SqlBinaryOperator}).
 *
 * <p>The list of nodes is immutable.
 */
public class SqlArrayWrapper extends SqlCall {
  SqlNode input;

  public SqlNode getInput() {
    return input;
  }

  public static final SqlOperator OPERATOR = new SqlArrayWrapperOperator();

  /**
   * Creates a new SqlArrayWrapper.
   * @param pos position
   * @param input input
   */
  public SqlArrayWrapper(SqlParserPos pos, SqlNode input) {
    super(pos);
    this.input = input;
  }


  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(input);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    assert i == 0;
    assert operand != null;
    this.input = operand;
  }

}
