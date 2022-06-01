<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

<#--
  Add implementations of additional parser statements, literals or
  data types.

  Example of SqlShowTables() implementation:
  SqlNode SqlShowTables()
  {
    ...local variables...
  }
  {
    <SHOW> <TABLES>
    ...
    {
      return SqlShowTables(...)
    }
  }
-->

/** Parses the ">>" operator used in Ha3SQL LeftMultiJoin */
void LoopEquals(List<Object> list, ExprContext exprContext, Span s) :
{
}
{
    <LOOP_EQUALS> {
        checkNonQueryExpression(exprContext);
        list.add(new SqlParserUtil.ToTreeListItem(Ha3SqlOperatorTable.LOOP_EQUALS, s.pos()));
    }
    Expression2b(ExprContext.ACCEPT_SUB_QUERY, list)
}

JoinType LeftMultiJoin() :
{
}
{
    <LEFT> <MULTI> <JOIN> { return JoinType.LEFT_MULTI_JOIN; }
}


/** Parses an ARRAY_WRAPPER clause following a table expression. */
SqlNode ArrayWrapper(SqlNode tableRef) :
{
    Span s;
}
{
    <ARRAY_WRAPPER>
    {
        s = span();
        return new SqlArrayWrapper(s.end(this), tableRef);
    }
}
