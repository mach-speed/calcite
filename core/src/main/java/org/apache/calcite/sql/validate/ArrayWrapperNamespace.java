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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlArrayWrapper;
import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * ArrayWrapperNamespace transforms all fields to array of its original type.
 */
public class ArrayWrapperNamespace extends AbstractNamespace {

  SqlArrayWrapper wrapper;

  public ArrayWrapperNamespace(SqlValidatorImpl validator, @Nullable SqlNode enclosingNode,
      SqlArrayWrapper wrapper) {
    super(validator, enclosingNode);
    this.wrapper = wrapper;
  }

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    return targetRowType;
  }

  @Override public RelDataType getRowType() {
    return super.getRowType();
  }

  @Override public @Nullable SqlNode getNode() {
    return wrapper;
  }
}
