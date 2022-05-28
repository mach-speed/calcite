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
package org.apache.calcite.sql.fun;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.Ha3SqlOperatorTable;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * SqlLoopEqualsOperator is the "loop equals" operator.
 * It is used to implement Ha3SQL
 */
public class SqlLoopEqualsOperator extends SqlBinaryOperator {
  public SqlLoopEqualsOperator() {
    super(">>", SqlKind.EQUALS, 94, true, ReturnTypes.BOOLEAN_NULLABLE, InferTypes.FIRST_KNOWN,
        null);
  }


  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    assert operands.size() == 2;
    final SqlNode left = operands.get(0);
    final SqlNode right = operands.get(1);

    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    RelDataType leftType = unwrapArrayType(validator, call, validator.deriveType(scope, left));
    RelDataType rightType = unwrapArrayType(validator, call, validator.deriveType(scope, right));

    SqlCallBinding callBinding = new SqlCallBinding(validator, scope, call);
    RelDataType leftRowType = SqlTypeUtil.promoteToRowType(typeFactory, leftType, null);
    RelDataType rightRowType = SqlTypeUtil.promoteToRowType(typeFactory, rightType, null);

    final ComparableOperandTypeChecker checker =
        (ComparableOperandTypeChecker)
            OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED;
    if (!checker.checkOperandTypes(
        new ExplicitOperatorBinding(
            callBinding,
            ImmutableList.of(leftRowType, rightRowType)), callBinding)) {
      throw validator.newValidationError(call,
          RESOURCE.incompatibleValueType(Ha3SqlOperatorTable.LOOP_EQUALS.getName()));
    }

    return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN),
        anyNullable(leftRowType.getFieldList()) || anyNullable(rightRowType.getFieldList()));
  }

  private static RelDataType unwrapArrayType(SqlValidator validator, SqlCall call,
      RelDataType type) {
    if (type instanceof ArraySqlType) {
      return ((ArraySqlType) type).getComponentType();
    }

//        throw validator.newValidationError(call, NebulaResource.RESOURCE.expectedArray());
    return type;
  }

  private static boolean anyNullable(List<RelDataTypeField> fieldList) {
    for (RelDataTypeField field : fieldList) {
      if (field.getType().isNullable()) {
        return true;
      }
    }
    return false;
  }

}
