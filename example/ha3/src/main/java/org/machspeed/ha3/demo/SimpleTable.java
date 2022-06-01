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
package org.machspeed.ha3.demo;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.adapter.file.CsvTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by machspeed on 2017/11/16.
 */
public class SimpleTable extends AbstractTable implements ScannableTable,
    ProjectableFilterableTable {

  private final String tableName;
  private final String filePath;
  private final List<String> fieldNames;
  private final List<SqlTypeName> sqlFieldTypes;

  private final List<RelDataType> relFieldTypes;

  private List<RelDataTypeField> fields;
  private SimpleTableStatistic statistic;

  private RelDataType rowType;

  private SimpleTable(
      String tableName,
      String filePath,
      List<String> fieldNames,
      List<SqlTypeName> sqlFieldTypes,
      SimpleTableStatistic statistic) {
    this.tableName = tableName;
    this.filePath = filePath;
    this.fieldNames = fieldNames;
    this.sqlFieldTypes = sqlFieldTypes;
    this.relFieldTypes = null;

    this.statistic = statistic;
  }

  private SimpleTable(String tableName,
      String filePath,
      List<RelDataTypeField> fields,
      SimpleTableStatistic statistic) {
    this.tableName = tableName;
    this.filePath = filePath;
    this.fields = fields;
    this.fieldNames = null;
    this.sqlFieldTypes = null;
    this.relFieldTypes = null;
    this.statistic = statistic;
  }

  public String getTableName() {
    return tableName;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      if (fields == null) {
        List<RelDataTypeField> tmpFields = new ArrayList<>(fieldNames.size());

        for (int i = 0; i < fieldNames.size(); i++) {
          RelDataType fieldType = typeFactory.createSqlType(sqlFieldTypes.get(i));
          RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
          tmpFields.add(field);
        }
        fields = tmpFields;
      }
      rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
    }
    return rowType;

  }

  @Override public Statistic getStatistic() {
    return statistic;
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    File file = new File(filePath);
    Source source = Sources.of(file);
    AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

    List<RelDataType> fieldTypes = getCsvFieldType();
    List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new CsvEnumerator<>(source, cancelFlag, false, null,
            CsvEnumerator.arrayConverter(fieldTypes, fields, false));
      }
    };
  }

  @Override public Enumerable<Object[]> scan(
      DataContext root, List<RexNode> filters, int[] projects) {
    File file = new File(filePath);
    Source source = Sources.of(file);
    AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

    List<RelDataType> fieldTypes = getCsvFieldType();
    List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new CsvEnumerator<>(source, cancelFlag, false, null,
            CsvEnumerator.arrayConverter(fieldTypes, fields, false));
      }
    };
  }

  private List<RelDataType> getCsvFieldType() {
    List<RelDataType> csvFieldTypes = new ArrayList<>(fields.size());
    fields.stream().map(f -> f.getType()).forEach(csvFieldTypes::add);
    return csvFieldTypes;
  }

  private static List<RelDataTypeField> constructFields(List<String> names,
      List<RelDataType> types) {
    int length = names.size();
    List<RelDataTypeField> fields = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      fields.add(new RelDataTypeFieldImpl(names.get(i), i, types.get(i)));
    }
    return fields;
  }

  public static Builder newBuilder(String tableName) {
    return new Builder(tableName);
  }

  /**
   * Builder for {@link CsvTable}.
   */
  public static final class Builder {

    private final String tableName;
    private String filePath;
    private final List<String> fieldNames = new ArrayList<>();
    private final List<SqlTypeName> sqlFieldTypes = new ArrayList<>();

    private final List<RelDataType> relDataTypes = new ArrayList<>();
    private long rowCount;

    private Builder(String tableName) {
      if (tableName == null || tableName.isEmpty()) {
        throw new IllegalArgumentException("Table name cannot be null or empty");
      }
      this.tableName = tableName;
    }

    public Builder addField(String name, SqlTypeName typeName) {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Field name cannot be null or empty");
      }
      if (fieldNames.contains(name)) {
        throw new IllegalArgumentException("Field already defined: " + name);
      }
      fieldNames.add(name);
      sqlFieldTypes.add(typeName);
      return this;
    }

    public Builder addField(String name, RelDataType relDataType) {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Field name cannot be null or empty");
      }
      if (fieldNames.contains(name)) {
        throw new IllegalArgumentException("Field already defined: " + name);
      }
      if (!sqlFieldTypes.isEmpty()) {
        throw new IllegalArgumentException("cannot use sqlType and relData at same time");
      }
      fieldNames.add(name);
      relDataTypes.add(relDataType);
      return this;
    }

    public Builder withFilePath(String filePath) {
      this.filePath = filePath;
      return this;
    }

    public Builder withRowCount(long rowCount) {
      this.rowCount = rowCount;
      return this;
    }

    public SimpleTable build() {
      if (fieldNames.isEmpty()) {
        throw new IllegalStateException("Table must have at least one field");
      }
      if (rowCount == 0L) {
        throw new IllegalStateException("Table must have positive row count");
      }
      if (!sqlFieldTypes.isEmpty()) {
        return new SimpleTable(
            tableName, filePath, fieldNames, sqlFieldTypes,
            new SimpleTableStatistic(rowCount));
      } else {
        return new SimpleTable(
            tableName, filePath, constructFields(fieldNames, relDataTypes),
            new SimpleTableStatistic(rowCount));

      }
    }
  }
}
