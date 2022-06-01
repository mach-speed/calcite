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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.Ha3SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.util.Collections;
import java.util.Properties;

/**
 * Demonstrates how to use the {@link Programs} class to create a
 * {@link Program} that converts a SQL query into a relational expression.
 */
public class SqlLoopJoinDemo {
  private SqlLoopJoinDemo() {
  }

  public static void main(String[] args) throws SqlParseException {
    final String sql =
        "select pv_id, user_id, ids, x, y "
            + " from backbone as mt "
            + " LEFT MULTI JOIN "
            + "feature ARRAY_WRAPPER as ft "
            + " ON mt.ids >> ft.id ";


    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    SimpleTable backbone = SimpleTable
        .newBuilder("backbone")
        .addField("pv_id",
        typeFactory.createSqlType(SqlTypeName.VARCHAR)).addField("user_id",
        typeFactory.createSqlType(SqlTypeName.VARCHAR)).addField("ids",
        typeFactory.createArrayType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), -1)).withRowCount(10).build();

    SimpleTable feature =
        SimpleTable.newBuilder("feature").addField("id", SqlTypeName.VARCHAR).addField("x",
            SqlTypeName.VARCHAR).addField("y", SqlTypeName.INTEGER).withRowCount(10).build();

    SimpleSchema schema = SimpleSchema.newBuilder("s").addTable(backbone).addTable(feature).build();

    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
    rootSchema.add(schema.getSchemaName(), schema);


    Properties configProperties = new Properties();
    configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        Boolean.TRUE.toString());
    configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(),
        Casing.UNCHANGED.toString());
    configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(),
        Casing.UNCHANGED.toString());
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);


// 创建CatalogReader, 用于指示如何读取Schema信息
    Prepare.CatalogReader catalogReader = new CalciteCatalogReader(rootSchema,
        Collections.singletonList(schema.getSchemaName()), typeFactory, config);

    SqlValidator.Config validatorConfig =
        SqlValidator
            .Config
            .DEFAULT
            .withLenientOperatorLookup(config.lenientOperatorLookup())
            .withConformance(config.conformance())
            .withDefaultNullCollation(config.defaultNullCollation())
            .withIdentifierExpansion(true);
    SqlValidator validator = SqlValidatorUtil.newValidator(Ha3SqlOperatorTable.instance(),
        catalogReader, typeFactory, validatorConfig);

    final SqlParser.Config parserConfig =
        SqlParser
            .config()
            .withParserFactory(SqlParserImpl.FACTORY)
            .withQuotedCasing(config.quotedCasing())
            .withUnquotedCasing(config.unquotedCasing())
            .withQuoting(config.quoting())
            .withConformance(config.conformance())
            .withCaseSensitive(config.caseSensitive());

    final SqlNode node = SqlParser.create(sql, parserConfig).parseQuery();
    System.out.println(node);
    System.out.println("\nparse sql node finished\n\n");

// 执行SQL验证
    SqlNode validateSqlNode = validator.validate(node);


    System.out.println(validateSqlNode);
    System.out.println("\nvalidate sql node finished\n\n");


    // 创建VolcanoPlanner, VolcanoPlanner在后面的优化中还需要用到
    VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
// 创建SqlToRelConverter
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    SqlToRelConverter.Config converterConfig =
        SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(false);
    SqlToRelConverter converter = new SqlToRelConverter(null, validator, catalogReader, cluster,
        StandardConvertletTable.INSTANCE, converterConfig);
// 将SqlNode树转化为RelNode树
    RelRoot root = converter.convertQuery(validateSqlNode, false, true);
    RelNode relNode = root.rel;

    System.out.println(relNode.explain());
    System.out.println("\nconvert sql node to relNode finished\n\n");


    // 4. SQL Optimize: RelNode --> RelNode
    RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC,
        CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE, CoreRules.FILTER_INTO_JOIN,
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
        EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE, EnumerableRules.ENUMERABLE_CALC_RULE,
        EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
    Program program = Programs.of(RuleSets.ofList(rules));
    RelNode afterOptimizeRelNode = program.run(planner, relNode, relNode.getTraitSet(),
        Collections.emptyList(), Collections.emptyList());
    System.out.println(afterOptimizeRelNode.explain());
    System.out.println("\nOptimize finished\n\n");


  }
}
