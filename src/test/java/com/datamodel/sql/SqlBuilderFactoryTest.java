package com.datamodel.sql;

import com.datamodel.common.BizException;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlBuilderFactoryTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SqlBuilderFactory factory = new SqlBuilderFactory(new DorisNameSanitizer());

    private JsonNode dag(String json) throws Exception {
        return mapper.readTree(json);
    }

    private static String source(String id, long dsId, String table, String fields) {
        return "{\"id\":\"" + id + "\",\"type\":\"source\",\"data\":{\"config\":{"
                + "\"dataSourceId\":" + dsId + ",\"tableName\":\"" + table + "\","
                + "\"fields\":[" + fields + "]}}}";
    }

    private static String field(String name, String type) {
        return "{\"name\":\"" + name + "\",\"type\":\"" + type + "\"}";
    }

    private static String edge(String id, String source, String target, String handle) {
        return "{\"id\":\"" + id + "\",\"source\":\"" + source + "\",\"target\":\"" + target
                + "\",\"sourceHandle\":\"out\",\"targetHandle\":\"" + handle + "\"}";
    }

    private static String nodesEdges(String nodes, String edges) {
        return "{\"nodes\":[" + nodes + "],\"edges\":[" + edges + "]}";
    }

    // ----------------------------------------------------------- Source

    @Test
    void sourceIsCommentOnly() throws Exception {
        String json = nodesEdges(source("node_1", 7, "orders", field("id", "BIGINT")), "");
        SqlBuildResult result = factory.build(dag(json), 101L, "node_1");
        assertEquals("-- 数据源副本表 dm_src_101_orders_e15999", result.getSql());
        assertEquals(result.getSql(), result.getDisplaySql());
        assertTrue(result.getOutputColumns().size() == 1);
        assertEquals("BIGINT", result.getOutputColumns().get(0).getDorisType());
    }

    // ----------------------------------------------------------- Filter

    @Test
    void filterEmptyProjectionUsesExplicitUpstreamColumns() throws Exception {
        String nodes = source("s1", 1, "orders", field("id", "BIGINT")) + ","
                + "{\"id\":\"f1\",\"type\":\"filter\",\"data\":{\"config\":"
                + "{\"alias\":\"a\",\"passthrough\":false,\"select\":[],\"conditions\":[]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "f1", "in-0"))), 101L, "f1");

        assertTrue(result.getSql().contains("`a`.`id`"), result.getSql());
        assertTrue(!result.getSql().contains("`a`.*"), result.getSql());
        assertTrue(result.getSql().contains("FROM dm_src_101_orders_aa1f98 AS `a`"), result.getSql());
        assertTrue(!result.getSql().contains("WHERE"), result.getSql());
        assertTrue(result.getParams().isEmpty());
    }

    @Test
    void filterNestedConditionsAreParameterized() throws Exception {
        String nodes = source("s1", 1, "orders", field("amt", "DECIMAL")) + ","
                + "{\"id\":\"f1\",\"type\":\"filter\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"passthrough\":false,"
                + "\"select\":[{\"field\":\"amt\",\"alias\":\"amt\",\"keep\":true,\"sort\":\"DESC\"}],"
                + "\"conditions\":["
                + "{\"logic\":\"AND\",\"indent\":0,\"left\":\"amt\",\"op\":\"GT\",\"rightType\":\"CONST\",\"right\":\"100\"},"
                + "{\"logic\":\"OR\",\"indent\":1,\"left\":\"status\",\"op\":\"EQ\",\"rightType\":\"CONST\",\"right\":\"PAID\"}"
                + "],\"limit\":1000}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "f1", "in-0"))), 101L, "f1");

        assertTrue(result.getSql().contains(
                "WHERE (`a`.`amt` > ?)\n  OR (`a`.`status` = ?)"), result.getSql());
        assertEquals(Arrays.asList(100L, "PAID"), result.getParams());
        assertTrue(result.getDisplaySql().contains(
                "WHERE (`a`.`amt` > 100)\n  OR (`a`.`status` = 'PAID')"), result.getDisplaySql());
        assertTrue(result.getSql().contains("ORDER BY `amt` DESC"), result.getSql());
        assertTrue(result.getSql().contains("LIMIT 1000"), result.getSql());
    }

    // ------------------------------------------------------------- Join

    @Test
    void joinUsesIn0AsLeftAndIn1AsRight() throws Exception {
        String nodes = source("s1", 1, "orders", field("uid", "INT")) + ","
                + source("s2", 2, "users", field("name", "VARCHAR")) + ","
                + "{\"id\":\"j1\",\"type\":\"join\",\"data\":{\"config\":{"
                + "\"leftAlias\":\"a\",\"rightAlias\":\"b\",\"joinType\":\"LEFT\","
                + "\"keys\":[{\"left\":\"uid\",\"right\":\"id\"}],"
                + "\"projection\":["
                + "{\"side\":\"a\",\"field\":\"uid\",\"alias\":\"uid\",\"keep\":true},"
                + "{\"side\":\"b\",\"field\":\"name\",\"alias\":\"user_name\",\"keep\":true}]}}}";
        String edges = edge("e2", "s2", "j1", "in-1") + "," + edge("e1", "s1", "j1", "in-0");
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes, edges)), 101L, "j1");

        assertTrue(result.getSql().contains("FROM dm_src_101_orders_aa1f98 AS `a`"), result.getSql());
        assertTrue(result.getSql().contains("LEFT JOIN dm_src_101_users_5e3c89 AS `b`"), result.getSql());
        assertTrue(result.getSql().contains("ON `a`.`uid` = `b`.`id`"), result.getSql());
        assertTrue(result.getSql().contains("`a`.`uid` AS `uid`"), result.getSql());
        assertTrue(result.getSql().contains("`b`.`name` AS `user_name`"), result.getSql());
    }

    // ------------------------------------------------------------ Union

    @Test
    void unionFillsMissingColumnsWithNull() throws Exception {
        String nodes = source("s1", 1, "orders",
                field("id", "BIGINT") + "," + field("memo", "VARCHAR")) + ","
                + source("s2", 2, "users", field("id", "BIGINT")) + ","
                + "{\"id\":\"u1\",\"type\":\"union\",\"data\":{\"config\":{"
                + "\"mode\":\"ALL\",\"align\":\"NAME\",\"outputs\":["
                + "{\"name\":\"id\",\"mapping\":{\"a\":\"id\",\"b\":\"id\"}},"
                + "{\"name\":\"note\",\"mapping\":{\"a\":\"memo\"}}"
                + "]}}}";
        String edges = edge("e1", "s1", "u1", "in-0") + "," + edge("e2", "s2", "u1", "in-1");
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes, edges)), 101L, "u1");

        assertTrue(result.getSql().contains("UNION ALL"), result.getSql());
        assertTrue(result.getSql().contains("`memo` AS `note`"), result.getSql());
        assertTrue(result.getSql().contains("NULL AS `note`"), result.getSql());
        assertEquals(2, result.getOutputColumns().size());
        assertEquals("BIGINT", result.getOutputColumns().get(0).getDorisType());
    }

    @Test
    void unionDistinctWrapsSubquery() throws Exception {
        String nodes = source("s1", 1, "orders", field("id", "BIGINT")) + ","
                + source("s2", 2, "users", field("id", "BIGINT")) + ","
                + "{\"id\":\"u1\",\"type\":\"union\",\"data\":{\"config\":{"
                + "\"mode\":\"DISTINCT\",\"align\":\"POSITION\"}}}";
        String edges = edge("e1", "s1", "u1", "in-0") + "," + edge("e2", "s2", "u1", "in-1");
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes, edges)), 101L, "u1");
        assertTrue(result.getSql().startsWith("SELECT DISTINCT * FROM ("), result.getSql());
        assertTrue(result.getSql().contains("`id`\nFROM dm_src_101_orders_aa1f98"), result.getSql());
        assertTrue(result.getSql().contains("`id`\nFROM dm_src_101_users_5e3c89"), result.getSql());
        assertEquals(1, result.getOutputColumns().size());
        assertEquals("BIGINT", result.getOutputColumns().get(0).getDorisType());
    }

    // ------------------------------------------------------------ GroupBy

    @Test
    void groupByBuildsGroupAndAggregates() throws Exception {
        String nodes = source("s1", 1, "orders",
                field("status", "VARCHAR") + "," + field("amt", "DECIMAL")) + ","
                + "{\"id\":\"g1\",\"type\":\"groupBy\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"globalAgg\":false,"
                + "\"groupBy\":[{\"field\":\"status\",\"alias\":\"status\"}],"
                + "\"aggregates\":[{\"field\":\"amt\",\"fn\":\"SUM\",\"alias\":\"total\"}]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "g1", "in-0"))), 101L, "g1");

        assertTrue(result.getSql().contains("`a`.`status`"), result.getSql());
        assertTrue(result.getSql().contains("SUM(`a`.`amt`) AS `total`"), result.getSql());
        assertTrue(result.getSql().contains("GROUP BY `a`.`status`"), result.getSql());
    }

    @Test
    void globalAggregateSkipsGroupBy() throws Exception {
        String nodes = source("s1", 1, "orders", field("amt", "DECIMAL")) + ","
                + "{\"id\":\"g1\",\"type\":\"groupBy\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"globalAgg\":true,\"aggregates\":"
                + "[{\"field\":\"amt\",\"fn\":\"AVG\",\"alias\":\"avg_amt\"}]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "g1", "in-0"))), 101L, "g1");
        assertTrue(!result.getSql().contains("GROUP BY"), result.getSql());
        assertTrue(result.getSql().contains("AVG(`a`.`amt`) AS `avg_amt`"), result.getSql());
    }

    // ---------------------------------------------------------- Convert

    @Test
    void convertBuildsExpressionsAndOutputColumns() throws Exception {
        String nodes = source("s1", 1, "orders",
                field("amount", "VARCHAR") + "," + field("created", "VARCHAR") + ","
                        + field("order_id", "BIGINT") + "," + field("note", "VARCHAR")) + ","
                + "{\"id\":\"c1\",\"type\":\"convert\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"conversions\":["
                + "{\"field\":\"amount\",\"targetType\":\"DECIMAL\",\"length\":10,\"precision\":2,"
                + "\"trim\":\"BOTH\",\"emptyToNull\":true},"
                + "{\"field\":\"created\",\"targetType\":\"DATE\","
                + "\"format\":\"%Y-%m-%d %H:%i:%s\"},"
                + "{\"field\":\"order_id\",\"targetType\":\"STRING\",\"output\":\"order_no\"},"
                + "{\"field\":\"note\",\"remove\":true}"
                + "]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "c1", "in-0"))), 101L, "c1");

        assertTrue(result.getSql().contains(
                "CAST(NULLIF(TRIM(`a`.`amount`),'') AS DECIMAL(10,2)) AS `amount`"),
                result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(STR_TO_DATE(`a`.`created`,'%Y-%m-%d %H:%i:%s') AS DATE) AS `created`"),
                result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(`a`.`order_id` AS STRING) AS `order_no`"), result.getSql());
        assertTrue(!result.getSql().contains("`a`.`note`"), result.getSql());
        assertTrue(result.getSql().contains("FROM dm_src_101_orders_aa1f98 AS `a`"), result.getSql());

        assertEquals(3, result.getOutputColumns().size());
        assertEquals("amount", result.getOutputColumns().get(0).getName());
        assertEquals("DECIMAL(10,2)", result.getOutputColumns().get(0).getDorisType());
        assertEquals("created", result.getOutputColumns().get(1).getName());
        assertEquals("DATE", result.getOutputColumns().get(1).getDorisType());
        assertEquals("order_no", result.getOutputColumns().get(2).getName());
        assertEquals("VARCHAR(255)", result.getOutputColumns().get(2).getDorisType());
    }

    @Test
    void convertEmptyConversionsProjectsExplicitUpstreamColumns() throws Exception {
        String nodes = source("s1", 1, "orders",
                field("id", "BIGINT") + "," + field("name", "VARCHAR")) + ","
                + "{\"id\":\"c1\",\"type\":\"convert\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"conversions\":[]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "c1", "in-0"))), 101L, "c1");

        assertTrue(result.getSql().contains("`a`.`id`"), result.getSql());
        assertTrue(result.getSql().contains("`a`.`name`"), result.getSql());
        assertTrue(!result.getSql().contains("`a`.*"), result.getSql());
        assertEquals(2, result.getOutputColumns().size());
    }

    @Test
    void convertRejectsUnknownField() throws Exception {
        String nodes = source("s1", 1, "orders", field("id", "BIGINT")) + ","
                + "{\"id\":\"c1\",\"type\":\"convert\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"conversions\":[{\"field\":\"missing\",\"targetType\":\"INT\"}]}}}";
        BizException e = assertThrows(BizException.class, () -> factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "c1", "in-0"))), 101L, "c1"));
        assertEquals(4001, e.getErrorCode().getCode());
        assertEquals("CONVERT_FIELD_UNKNOWN",
                ((java.util.Map<?, ?>) e.getData()).get("reason"));
        assertTrue(e.getMessage().contains("missing"), e.getMessage());
    }

    @Test
    void convertRejectsDuplicateOutput() throws Exception {
        String nodes = source("s1", 1, "orders",
                field("id", "BIGINT") + "," + field("uid", "BIGINT")) + ","
                + "{\"id\":\"c1\",\"type\":\"convert\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"conversions\":["
                + "{\"field\":\"id\",\"targetType\":\"STRING\",\"output\":\"key\"},"
                + "{\"field\":\"uid\",\"targetType\":\"STRING\",\"output\":\"key\"}]}}}";
        BizException e = assertThrows(BizException.class, () -> factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "c1", "in-0"))), 101L, "c1"));
        assertEquals("CONVERT_DUPLICATE_OUTPUT",
                ((java.util.Map<?, ?>) e.getData()).get("reason"));
    }

    @Test
    void convertRejectsUnsupportedType() throws Exception {
        String nodes = source("s1", 1, "orders", field("id", "BIGINT")) + ","
                + "{\"id\":\"c1\",\"type\":\"convert\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"conversions\":[{\"field\":\"id\",\"targetType\":\"TIMESTAMP\"}]}}}";
        BizException e = assertThrows(BizException.class, () -> factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "c1", "in-0"))), 101L, "c1"));
        assertEquals("CONVERT_TYPE_UNSUPPORTED",
                ((java.util.Map<?, ?>) e.getData()).get("reason"));
        assertTrue(e.getMessage().contains("TIMESTAMP"), e.getMessage());
    }

    // -------------------------------------------------- upstream missing

    @Test
    void missingUpstreamIsReportedNotThrown() throws Exception {
        String nodes = "{\"id\":\"f1\",\"type\":\"filter\",\"data\":{\"config\":{\"alias\":\"a\"}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes, "")), 101L, "f1");
        assertTrue(result.getSql().contains("<missing_upstream>"), result.getSql());
        assertTrue(result.getUpstreamMissing().contains("f1:no-upstream"),
                result.getUpstreamMissing().toString());
    }

    @Test
    void unknownTargetNodeIsDagError() throws Exception {
        String nodes = source("s1", 1, "orders", field("id", "BIGINT"));
        BizException e = assertThrows(BizException.class,
                () -> factory.build(dag(nodesEdges(nodes, "")), 101L, "nope"));
        assertEquals(4001, e.getErrorCode().getCode());
    }

    @Test
    void numericTypeInferenceUsesDecimal() throws Exception {
        String nodes = source("s1", 1, "orders",
                field("amt", "DECIMAL") + "," + field("cnt", "INT")) + ","
                + "{\"id\":\"g1\",\"type\":\"groupBy\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"globalAgg\":true,\"aggregates\":["
                + "{\"field\":\"amt\",\"fn\":\"MAX\",\"alias\":\"max_amt\"},"
                + "{\"field\":\"cnt\",\"fn\":\"COUNT_DISTINCT\",\"alias\":\"cnts\"}]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "g1", "in-0"))), 101L, "g1");
        assertEquals("DECIMAL", result.getOutputColumns().get(0).getDorisType());
        assertEquals("BIGINT", result.getOutputColumns().get(1).getDorisType());
    }

    @Test
    void literalNumbersAreNotQuoted() throws Exception {
        Condition row = Condition.parse(mapper.readTree(
                "{\"left\":\"amt\",\"op\":\"GT\",\"rightType\":\"CONST\",\"right\":\"12.5\"}"));
        ConditionCompiler.Compiled compiled = ConditionCompiler.compile(
                Arrays.asList(row), "a", Identifiers.QuoteStyle.BACKTICK, false);
        assertEquals("(`a`.`amt` > 12.5)", compiled.getExpr());
    }

    // ------------------------------------------------------- Calculator

    private static final String CALC_FIELDS = field("a", "DOUBLE") + "," + field("b", "DOUBLE")
            + "," + field("c", "DOUBLE") + "," + field("d", "DOUBLE") + ","
            + field("s1", "VARCHAR") + "," + field("s2", "VARCHAR");

    private JsonNode calculatorDag(String calculations) throws Exception {
        String nodes = source("s1", 1, "calc_src", CALC_FIELDS) + ","
                + "{\"id\":\"k1\",\"type\":\"calculator\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"calculations\":[" + calculations + "]}}}";
        return dag(nodesEdges(nodes, edge("e1", "s1", "k1", "in-0")));
    }

    private JsonNode formulaDag(String expression) throws Exception {
        return formulaDag("x", expression, "DOUBLE");
    }

    private JsonNode formulaDag(String output, String expression, String returnType) throws Exception {
        String nodes = source("s1", 1, "calc_src", CALC_FIELDS) + ","
                + "{\"id\":\"f1\",\"type\":\"formula\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"formulas\":[{\"output\":" + mapper.writeValueAsString(output)
                + ",\"expression\":" + mapper.writeValueAsString(expression)
                + ",\"returnType\":" + mapper.writeValueAsString(returnType) + "}]}}}";
        return dag(nodesEdges(nodes, edge("e1", "s1", "f1", "in-0")));
    }

    private static Object reason(BizException e) {
        return ((java.util.Map<?, ?>) e.getData()).get("reason");
    }

    @Test
    void calculatorBuildsOperationsAndOutputColumns() throws Exception {
        String calculations = ""
                + "{\"output\":\"sum_ab\",\"operation\":\"ADD\",\"fieldA\":\"A\",\"fieldB\":\"b\","
                + "\"fieldC\":null,\"returnType\":\"DOUBLE\",\"length\":null,\"precision\":null},"
                + "{\"output\":\"sum_abc\",\"operation\":\"ADD3\",\"fieldA\":\"a\",\"fieldB\":\"b\","
                + "\"fieldC\":\"c\"},"
                + "{\"output\":\"product\",\"operation\":\"MULTIPLY\",\"fieldA\":\"a\","
                + "\"fieldB\":\"b\",\"returnType\":\"BIGINT\"},"
                + "{\"output\":\"ratio\",\"operation\":\"DIVIDE\",\"fieldA\":\"a\","
                + "\"fieldB\":\"b\",\"returnType\":\"DECIMAL\",\"length\":10,\"precision\":2},"
                + "{\"output\":\"diff\",\"operation\":\"SUBTRACT\",\"fieldA\":\"a\",\"fieldB\":\"b\"},"
                + "{\"output\":\"rem\",\"operation\":\"MOD\",\"fieldA\":\"a\",\"fieldB\":\"b\"}";
        SqlBuildResult result = factory.build(calculatorDag(calculations), 101L, "k1");

        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` + `a`.`b` AS DOUBLE) AS `sum_ab`"), result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` + `a`.`b` + `a`.`c` AS DOUBLE) AS `sum_abc`"), result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` * `a`.`b` AS BIGINT) AS `product`"), result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` / `a`.`b` AS DECIMAL(10,2)) AS `ratio`"), result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` - `a`.`b` AS DOUBLE) AS `diff`"), result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` % `a`.`b` AS DOUBLE) AS `rem`"), result.getSql());
        assertTrue(!result.getSql().contains("`a`.*"), result.getSql());

        assertEquals(12, result.getOutputColumns().size());
        assertEquals("sum_ab", result.getOutputColumns().get(6).getName());
        assertEquals("DOUBLE", result.getOutputColumns().get(6).getDorisType());
        assertEquals("sum_abc", result.getOutputColumns().get(7).getName());
        assertEquals("DOUBLE", result.getOutputColumns().get(7).getDorisType());
        assertEquals("product", result.getOutputColumns().get(8).getName());
        assertEquals("BIGINT", result.getOutputColumns().get(8).getDorisType());
        assertEquals("ratio", result.getOutputColumns().get(9).getName());
        assertEquals("DECIMAL(10,2)", result.getOutputColumns().get(9).getDorisType());
        assertEquals("diff", result.getOutputColumns().get(10).getName());
        assertEquals("DOUBLE", result.getOutputColumns().get(10).getDorisType());
        assertEquals("rem", result.getOutputColumns().get(11).getName());
        assertEquals("DOUBLE", result.getOutputColumns().get(11).getDorisType());
        assertEquals("a", result.getOutputColumns().get(0).getName());
    }

    @Test
    void calculatorAllowsChineseOutputName() throws Exception {
        SqlBuildResult result = factory.build(calculatorDag(
                "{\"output\":\"合计\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"b\"}"),
                101L, "k1");
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` + `a`.`b` AS DOUBLE) AS `合计`"), result.getSql());
        assertEquals("合计", result.getOutputColumns().get(6).getName());
    }

    @Test
    void calculatorEmptyConfigProjectsUpstreamColumns() throws Exception {
        SqlBuildResult result = factory.build(calculatorDag(""), 101L, "k1");

        assertTrue(result.getSql().contains("`a`.`a`"), result.getSql());
        assertTrue(result.getSql().contains("`a`.`s2`"), result.getSql());
        assertTrue(!result.getSql().contains("`a`.*"), result.getSql());
        assertTrue(!result.getSql().contains("CAST("), result.getSql());
        assertEquals(6, result.getOutputColumns().size());
    }

    @Test
    void calculatorRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"output\":\"x\",\"operation\":\"POW\",\"fieldA\":\"a\",\"fieldB\":\"b\"}",
                        "CALC_OPERATION_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"\",\"fieldA\":\"a\",\"fieldB\":\"b\"}",
                        "CALC_OPERATION_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"ADD\",\"fieldA\":\"a\"}",
                        "CALC_FIELD_MISSING"},
                {"{\"output\":\"x\",\"operation\":\"ADD3\",\"fieldA\":\"a\",\"fieldB\":\"b\"}",
                        "CALC_FIELD_MISSING"},
                {"{\"output\":\"x\",\"operation\":\"ADD\",\"fieldA\":\"nope\",\"fieldB\":\"b\"}",
                        "CALC_FIELD_UNKNOWN"},
                {"{\"output\":\"bad`x\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"b\"}",
                        "CALC_OUTPUT_INVALID"},
                {"{\"output\":\"\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"b\"}",
                        "CALC_OUTPUT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"b\","
                        + "\"returnType\":\"FLOAT\"}", "CALC_OUTPUT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"b\","
                        + "\"returnType\":\"DECIMAL\",\"length\":39,\"precision\":2}",
                        "CALC_OUTPUT_INVALID"},
                {"{\"output\":\"A\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"b\"}",
                        "CALC_OUTPUT_DUPLICATE"},
                {"{\"output\":\"x\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"b\"},"
                        + "{\"output\":\"x\",\"operation\":\"ADD\",\"fieldA\":\"a\",\"fieldB\":\"c\"}",
                        "CALC_OUTPUT_DUPLICATE"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(calculatorDag(c[0]), 101L, "k1"), c[0]);
            assertEquals(4001, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    @Test
    void calculatorBuildsExtendedOperations() throws Exception {
        String calculations = ""
                + "{\"output\":\"round_a\",\"operation\":\"ROUND\",\"fieldA\":\"a\"},"
                + "{\"output\":\"std_ab\",\"operation\":\"STDROUND\",\"fieldA\":\"a\","
                + "\"argBType\":\"LITERAL\",\"literalB\":\"2\"},"
                + "{\"output\":\"ceil_a\",\"operation\":\"CEIL\",\"fieldA\":\"a\",\"returnType\":\"INT\"},"
                + "{\"output\":\"floor_a\",\"operation\":\"FLOOR\",\"fieldA\":\"a\"},"
                + "{\"output\":\"nvl_s\",\"operation\":\"NVL\",\"fieldA\":\"s1\",\"fieldB\":\"s2\","
                + "\"returnType\":\"STRING\",\"length\":20},"
                + "{\"output\":\"min_ab\",\"operation\":\"MIN\",\"fieldA\":\"a\",\"fieldB\":\"b\"},"
                + "{\"output\":\"max_ab\",\"operation\":\"MAX\",\"fieldA\":\"a\",\"fieldB\":\"b\"},"
                + "{\"output\":\"avg3\",\"operation\":\"AVG3\",\"fieldA\":\"a\",\"fieldB\":\"b\","
                + "\"fieldC\":\"c\"},"
                + "{\"output\":\"d_add\",\"operation\":\"DATE_ADD_DAYS\",\"fieldA\":\"d\","
                + "\"literalB\":\"3\",\"returnType\":\"DATETIME\"},"
                + "{\"output\":\"d_sub\",\"operation\":\"DATE_SUB_MONTHS\",\"fieldA\":\"d\","
                + "\"literalB\":\"1\",\"returnType\":\"DATE\"},"
                + "{\"output\":\"d_diff\",\"operation\":\"DATE_DIFF_HOURS\",\"fieldA\":\"d\","
                + "\"fieldB\":\"d\",\"returnType\":\"BIGINT\"},"
                + "{\"output\":\"yr\",\"operation\":\"YEAR_OF_DATE\",\"fieldA\":\"d\","
                + "\"returnType\":\"INT\"},"
                + "{\"output\":\"fmt\",\"operation\":\"DATE_FORMAT\",\"fieldA\":\"d\","
                + "\"literalB\":\"%Y-%m-%d\"},"
                + "{\"output\":\"parsed\",\"operation\":\"DATE_PARSE\",\"fieldA\":\"s1\","
                + "\"literalB\":\"%Y-%m-%d\",\"returnType\":\"DATETIME\"},"
                + "{\"output\":\"upper_s\",\"operation\":\"UPPER\",\"fieldA\":\"s1\"},"
                + "{\"output\":\"lower_s\",\"operation\":\"LOWER\",\"fieldA\":\"s1\"},"
                + "{\"output\":\"init_s\",\"operation\":\"INITCAP\",\"fieldA\":\"s1\"},"
                + "{\"output\":\"trim_s\",\"operation\":\"TRIM\",\"fieldA\":\"s1\"},"
                + "{\"output\":\"len_s\",\"operation\":\"LENGTH\",\"fieldA\":\"s1\","
                + "\"returnType\":\"BIGINT\"},"
                + "{\"output\":\"sub_s\",\"operation\":\"SUBSTRING\",\"fieldA\":\"s1\","
                + "\"literalB\":\"2\",\"literalC\":\"3\"},"
                + "{\"output\":\"left_s\",\"operation\":\"LEFT\",\"fieldA\":\"s1\","
                + "\"argBType\":\"LITERAL\",\"literalB\":\"2\"},"
                + "{\"output\":\"right_s\",\"operation\":\"RIGHT\",\"fieldA\":\"s1\","
                + "\"argBType\":\"LITERAL\",\"literalB\":\"2\"},"
                + "{\"output\":\"rep_s\",\"operation\":\"REPLACE\",\"fieldA\":\"s1\","
                + "\"literalB\":\"O'Reilly\",\"literalC\":\"X\"},"
                + "{\"output\":\"cat3\",\"operation\":\"CONCAT3\",\"fieldA\":\"s1\","
                + "\"fieldB\":\"s2\",\"fieldC\":\"s1\"},"
                + "{\"output\":\"lpad_s\",\"operation\":\"LPAD\",\"fieldA\":\"s1\","
                + "\"literalB\":\"6\",\"literalC\":\"0\"},"
                + "{\"output\":\"rpad_s\",\"operation\":\"RPAD\",\"fieldA\":\"s1\","
                + "\"literalB\":\"6\",\"literalC\":\"0\"}";
        SqlBuildResult result = factory.build(calculatorDag(calculations), 101L, "k1");
        String sql = result.getSql();

        assertTrue(sql.contains("CAST(ROUND_BANKERS(`a`.`a`) AS DOUBLE) AS `round_a`"), sql);
        assertTrue(sql.contains("CAST(ROUND(`a`.`a`, '2') AS DOUBLE) AS `std_ab`"), sql);
        assertTrue(sql.contains("CAST(CEIL(`a`.`a`) AS INT) AS `ceil_a`"), sql);
        assertTrue(sql.contains("CAST(FLOOR(`a`.`a`) AS DOUBLE) AS `floor_a`"), sql);
        assertTrue(sql.contains("CAST(IFNULL(`a`.`s1`, `a`.`s2`) AS STRING) AS `nvl_s`"), sql);
        assertTrue(sql.contains("CAST(LEAST(`a`.`a`, `a`.`b`) AS DOUBLE) AS `min_ab`"), sql);
        assertTrue(sql.contains("CAST(GREATEST(`a`.`a`, `a`.`b`) AS DOUBLE) AS `max_ab`"), sql);
        assertTrue(sql.contains(
                "CAST((`a`.`a` + `a`.`b` + `a`.`c`) / 3 AS DOUBLE) AS `avg3`"), sql);
        assertTrue(sql.contains(
                "CAST(DATE_ADD(`a`.`d`, INTERVAL '3' DAY) AS DATETIME) AS `d_add`"), sql);
        assertTrue(sql.contains(
                "CAST(DATE_SUB(`a`.`d`, INTERVAL '1' MONTH) AS DATE) AS `d_sub`"), sql);
        assertTrue(sql.contains(
                "CAST(TIMESTAMPDIFF(HOUR, `a`.`d`, `a`.`d`) AS BIGINT) AS `d_diff`"), sql);
        assertTrue(sql.contains("CAST(YEAR(`a`.`d`) AS INT) AS `yr`"), sql);
        assertTrue(sql.contains(
                "CAST(DATE_FORMAT(`a`.`d`, '%Y-%m-%d') AS STRING) AS `fmt`"), sql);
        assertTrue(sql.contains(
                "CAST(STR_TO_DATE(`a`.`s1`, '%Y-%m-%d') AS DATETIME) AS `parsed`"), sql);
        assertTrue(sql.contains("CAST(UPPER(`a`.`s1`) AS STRING) AS `upper_s`"), sql);
        assertTrue(sql.contains("CAST(LOWER(`a`.`s1`) AS STRING) AS `lower_s`"), sql);
        assertTrue(sql.contains("CAST(INITCAP(`a`.`s1`) AS STRING) AS `init_s`"), sql);
        assertTrue(sql.contains("CAST(TRIM(`a`.`s1`) AS STRING) AS `trim_s`"), sql);
        assertTrue(sql.contains("CAST(LENGTH(`a`.`s1`) AS BIGINT) AS `len_s`"), sql);
        assertTrue(sql.contains(
                "CAST(SUBSTRING(`a`.`s1`, '2', '3') AS STRING) AS `sub_s`"), sql);
        assertTrue(sql.contains("CAST(LEFT(`a`.`s1`, '2') AS STRING) AS `left_s`"), sql);
        assertTrue(sql.contains("CAST(RIGHT(`a`.`s1`, '2') AS STRING) AS `right_s`"), sql);
        assertTrue(sql.contains(
                "CAST(REPLACE(`a`.`s1`, 'O''Reilly', 'X') AS STRING) AS `rep_s`"), sql);
        assertTrue(sql.contains(
                "CAST(CONCAT(`a`.`s1`, `a`.`s2`, `a`.`s1`) AS STRING) AS `cat3`"), sql);
        assertTrue(sql.contains(
                "CAST(LPAD(`a`.`s1`, '6', '0') AS STRING) AS `lpad_s`"), sql);
        assertTrue(sql.contains(
                "CAST(RPAD(`a`.`s1`, '6', '0') AS STRING) AS `rpad_s`"), sql);

        assertEquals(6 + 26, result.getOutputColumns().size());
        assertEquals("VARCHAR(20)", result.getOutputColumns().get(10).getDorisType());
        assertEquals("VARCHAR(255)", result.getOutputColumns().get(18).getDorisType());
        assertEquals("DATETIME", result.getOutputColumns().get(19).getDorisType());
    }

    @Test
    void calculatorRejectsInvalidArgumentsAndTypes() throws Exception {
        String[][] cases = {
                {"{\"output\":\"x\",\"operation\":\"DATE_FORMAT\",\"fieldA\":\"d\","
                        + "\"argBType\":\"FIELD\",\"fieldB\":\"d\"}", "CALC_ARGUMENT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"DATE_FORMAT\",\"fieldA\":\"d\"}",
                        "CALC_ARGUMENT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"DATE_PARSE\",\"fieldA\":\"s1\","
                        + "\"argBType\":\"LITERAL\"}", "CALC_ARGUMENT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"SUBSTRING\",\"fieldA\":\"s1\","
                        + "\"literalB\":\"2\",\"argCType\":\"EXPR\"}", "CALC_ARGUMENT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"ROUND\",\"fieldA\":\"a\",\"fieldB\":\"b\","
                        + "\"argBType\":\"LITERAL\"}", "CALC_ARGUMENT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"CEIL\",\"fieldA\":\"a\","
                        + "\"returnType\":\"STRING\"}", "CALC_OUTPUT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"DATE_DIFF_DAYS\",\"fieldA\":\"d\","
                        + "\"fieldB\":\"d\",\"returnType\":\"DOUBLE\"}", "CALC_OUTPUT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"DATE_PARSE\",\"fieldA\":\"s1\","
                        + "\"literalB\":\"%Y\",\"returnType\":\"STRING\"}", "CALC_OUTPUT_INVALID"},
                {"{\"output\":\"x\",\"operation\":\"AVG3\",\"fieldA\":\"a\",\"fieldB\":\"b\"}",
                        "CALC_FIELD_MISSING"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(calculatorDag(c[0]), 101L, "k1"), c[0]);
            assertEquals(4001, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // ---------------------------------------------------------- Formula

    @Test
    void formulaBuildsExpressionsAndOutputColumns() throws Exception {
        String nodes = source("s1", 1, "calc_src", CALC_FIELDS) + ","
                + "{\"id\":\"f1\",\"type\":\"formula\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"formulas\":["
                + "{\"output\":\"total\",\"expression\":\"[a]+[b]+[c]+[d]\",\"returnType\":\"DOUBLE\"},"
                + "{\"output\":\"sum4\",\"expression\":\"SUM([a],[b],[c],[d])\"},"
                + "{\"output\":\"ratio\",\"expression\":\"ROUND([a]/[b],2)\"},"
                + "{\"output\":\"merged\",\"expression\":\"CONCAT([s1],'-',[s2])\",\"returnType\":\"STRING\"},"
                + "{\"output\":\"amount\",\"expression\":\"[a]\",\"returnType\":\"DECIMAL\"}"
                + "]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "f1", "in-0"))), 101L, "f1");

        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` + `a`.`b` + `a`.`c` + `a`.`d` AS DOUBLE) AS `total`"),
                result.getSql());
        assertTrue(result.getSql().contains(
                "CAST((`a`.`a` + `a`.`b` + `a`.`c` + `a`.`d`) AS DOUBLE) AS `sum4`"),
                result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(ROUND(`a`.`a` / `a`.`b`, 2) AS DOUBLE) AS `ratio`"), result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(CONCAT(`a`.`s1`, '-', `a`.`s2`) AS STRING) AS `merged`"), result.getSql());
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` AS DECIMAL(18,2)) AS `amount`"), result.getSql());
        assertTrue(!result.getSql().contains("`a`.*"), result.getSql());

        assertEquals(11, result.getOutputColumns().size());
        assertEquals("total", result.getOutputColumns().get(6).getName());
        assertEquals("DOUBLE", result.getOutputColumns().get(6).getDorisType());
        assertEquals("merged", result.getOutputColumns().get(9).getName());
        assertEquals("VARCHAR(255)", result.getOutputColumns().get(9).getDorisType());
        assertEquals("amount", result.getOutputColumns().get(10).getName());
        assertEquals("DECIMAL(18,2)", result.getOutputColumns().get(10).getDorisType());
    }

    @Test
    void formulaEmptyConfigProjectsUpstreamColumns() throws Exception {
        String nodes = source("s1", 1, "calc_src", CALC_FIELDS) + ","
                + "{\"id\":\"f1\",\"type\":\"formula\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"formulas\":[]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "f1", "in-0"))), 101L, "f1");

        assertTrue(result.getSql().contains("`a`.`a`"), result.getSql());
        assertTrue(result.getSql().contains("`a`.`s2`"), result.getSql());
        assertTrue(!result.getSql().contains("`a`.*"), result.getSql());
        assertTrue(!result.getSql().contains("CAST("), result.getSql());
        assertEquals(6, result.getOutputColumns().size());
    }

    @Test
    void formulaKeepsOperatorPrecedence() throws Exception {
        SqlBuildResult result = factory.build(
                formulaDag("[a]+[b]*([c]-[d])"), 101L, "f1");
        assertTrue(result.getSql().contains(
                "CAST(`a`.`a` + `a`.`b` * (`a`.`c` - `a`.`d`) AS DOUBLE) AS `x`"),
                result.getSql());

        SqlBuildResult sub = factory.build(formulaDag("[a]-([b]-[c])"), 101L, "f1");
        assertTrue(sub.getSql().contains(
                "CAST(`a`.`a` - (`a`.`b` - `a`.`c`) AS DOUBLE) AS `x`"), sub.getSql());
    }

    @Test
    void formulaRejectsInvalidExpressions() throws Exception {
        String[][] cases = {
                {"", "FORMULA_EXPRESSION_EMPTY"},
                {"[a]+", "FORMULA_SYNTAX"},
                {"[a]+[b", "FORMULA_SYNTAX"},
                {"[nope]+[a]", "FORMULA_FIELD_UNKNOWN"},
                {"COUNT([a])", "FORMULA_AGGREGATE_UNSUPPORTED"},
                {"AVG([a])", "FORMULA_AGGREGATE_UNSUPPORTED"},
                {"STDDEV_POP([a])", "FORMULA_AGGREGATE_UNSUPPORTED"},
                {"VAR_SAMP([a])", "FORMULA_AGGREGATE_UNSUPPORTED"},
                {"MEDIAN([a])", "FORMULA_AGGREGATE_UNSUPPORTED"},
                {"FOO([a])", "FORMULA_FUNCTION_UNSUPPORTED"},
                {"[a]; DROP TABLE t", "FORMULA_SYNTAX"},
                {"[a]--x", "FORMULA_SYNTAX"},
                {"[a]/*x*/", "FORMULA_SYNTAX"},
                {"[a]@[b]", "FORMULA_SYNTAX"},
                {"[a]?[b]", "FORMULA_SYNTAX"},
                {"[a]::INT", "FORMULA_SYNTAX"},
                {"SELECT [a]", "FORMULA_SYNTAX"},
                {"(SELECT 1)", "FORMULA_SYNTAX"},
                {"[a].[b]", "FORMULA_SYNTAX"},
                {"SUM([a])", "FORMULA_SYNTAX"},
                {repeat("a", 2001), "FORMULA_SYNTAX"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(formulaDag(c[0]), 101L, "f1"), c[0]);
            assertEquals(4001, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    @Test
    void formulaSupportsDateAndStringFunctions() throws Exception {
        SqlBuildResult nvl = factory.build(
                formulaDag("nvl_x", "NVL([a],[b])", "DOUBLE"), 101L, "f1");
        assertTrue(nvl.getSql().contains(
                "CAST(IFNULL(`a`.`a`, `a`.`b`) AS DOUBLE) AS `nvl_x`"), nvl.getSql());

        SqlBuildResult fmt = factory.build(
                formulaDag("d_fmt", "DATE_FORMAT([d],'%Y-%m-%d')", "STRING"), 101L, "f1");
        assertTrue(fmt.getSql().contains(
                "CAST(DATE_FORMAT(`a`.`d`, '%Y-%m-%d') AS STRING) AS `d_fmt`"), fmt.getSql());

        SqlBuildResult diff = factory.build(
                formulaDag("d_diff", "DATEDIFF([d],[d])", "BIGINT"), 101L, "f1");
        assertTrue(diff.getSql().contains(
                "CAST(DATEDIFF(`a`.`d`, `a`.`d`) AS BIGINT) AS `d_diff`"), diff.getSql());

        SqlBuildResult year = factory.build(
                formulaDag("d_year", "YEAR([d])", "INT"), 101L, "f1");
        assertTrue(year.getSql().contains("CAST(YEAR(`a`.`d`) AS INT) AS `d_year`"), year.getSql());

        SqlBuildResult left = factory.build(
                formulaDag("s_left", "LEFT([s1],3)", "STRING"), 101L, "f1");
        assertTrue(left.getSql().contains(
                "CAST(LEFT(`a`.`s1`, 3) AS STRING) AS `s_left`"), left.getSql());

        SqlBuildResult pad = factory.build(
                formulaDag("s_pad", "LPAD([s1],5,'0')", "STRING"), 101L, "f1");
        assertTrue(pad.getSql().contains(
                "CAST(LPAD(`a`.`s1`, 5, '0') AS STRING) AS `s_pad`"), pad.getSql());
    }

    @Test
    void formulaKeepsAggregateBlacklist() throws Exception {
        for (String expression : new String[]{"MIN([a])", "MAX([a])", "AVG([a])"}) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(formulaDag(expression), 101L, "f1"), expression);
            assertEquals("FORMULA_AGGREGATE_UNSUPPORTED", reason(e), expression);
        }
    }

    @Test
    void formulaRejectsInvalidOutputs() throws Exception {
        BizException duplicated = assertThrows(BizException.class,
                () -> factory.build(formulaDag("A", "[a]+[b]", "DOUBLE"), 101L, "f1"));
        assertEquals("FORMULA_OUTPUT_DUPLICATE", reason(duplicated));

        BizException invalid = assertThrows(BizException.class,
                () -> factory.build(formulaDag("bad`x", "[a]+[b]", "DOUBLE"), 101L, "f1"));
        assertEquals("FORMULA_OUTPUT_INVALID", reason(invalid));

        BizException empty = assertThrows(BizException.class,
                () -> factory.build(formulaDag("  ", "[a]+[b]", "DOUBLE"), 101L, "f1"));
        assertEquals("FORMULA_OUTPUT_INVALID", reason(empty));

        BizException type = assertThrows(BizException.class,
                () -> factory.build(formulaDag("x", "[a]+[b]", "FLOAT"), 101L, "f1"));
        assertEquals("FORMULA_OUTPUT_INVALID", reason(type));
    }

    private static String repeat(String value, int times) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append(value);
        }
        return sb.toString();
    }
}
