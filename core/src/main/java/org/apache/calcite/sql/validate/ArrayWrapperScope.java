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

import org.apache.calcite.sql.SqlArrayWrapper;
import org.apache.calcite.sql.SqlNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Scope for {@link SqlArrayWrapper}.
 */
public class ArrayWrapperScope extends ListScope {
  private final @Nullable SqlValidatorScope usingScope;

  private final SqlArrayWrapper wrapper;

  public ArrayWrapperScope(
      SqlValidatorScope parent,
      @Nullable SqlValidatorScope usingScope,
      SqlArrayWrapper wrapper) {
    super(parent);
    this.usingScope = usingScope;
    this.wrapper = wrapper;
  }

  @Override public SqlNode getNode() {
    return wrapper;
  }

  @Override public boolean isWithin(@Nullable SqlValidatorScope scope2) {
    if (this == scope2) {
      return true;
    }
    // go from the JOIN to the enclosing SELECT
    return requireNonNull(usingScope, "usingScope").isWithin(scope2);
  }
}
