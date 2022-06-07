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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * <code>LogicalArrayWrapper</code> wrap its input type.
 */
public class LogicalArrayWrapper extends SingleRel implements Hintable {

  protected final RelDataType rowType;

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits  TraitSet
   * @param input   Input relational expression
   */
  protected LogicalArrayWrapper(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    rowType = typeFactory.wrapEachField(input.getRowType());
  }

  @Override public String explain() {
    return super.explain();
  }

  @Override public boolean fieldIsNullable(int i) {
    return super.fieldIsNullable(i);
  }

  @Override public RelNode attachHints(List<RelHint> hintList) {
    return Hintable.super.attachHints(hintList);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return Hintable.super.withHints(hintList);
  }

  @Override public ImmutableList<RelHint> getHints() {
    return null;
  }

  @Override protected RelDataType deriveRowType() {
    assert rowType != null;
    return rowType;
  }

  public static LogicalArrayWrapper create(RelNode input) {
    return new LogicalArrayWrapper(input.getCluster(), input.getTraitSet(), input);
  }
}
