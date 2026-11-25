package com.datamodel.sql;

import com.datamodel.common.BizException;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
                field("amt", "DECIMAL(18,2)") + "," + field("cnt", "INT")) + ","
                + "{\"id\":\"g1\",\"type\":\"groupBy\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"globalAgg\":true,\"aggregates\":["
                + "{\"field\":\"amt\",\"fn\":\"MAX\",\"alias\":\"max_amt\"},"
                + "{\"field\":\"cnt\",\"fn\":\"COUNT_DISTINCT\",\"alias\":\"cnts\"}]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "g1", "in-0"))), 101L, "g1");
        assertEquals("DECIMAL(18,2)", result.getOutputColumns().get(0).getDorisType());
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

    // ----------------------------------------------------- ReplaceNull

    private static final String OPS_FIELDS = field("uid", "BIGINT") + ","
            + field("amount", "DECIMAL(18,2)") + "," + field("city", "VARCHAR(64)") + ","
            + field("note", "VARCHAR(64)") + "," + field("tags", "VARCHAR(64)") + ","
            + field("path", "STRING");

    private JsonNode opsDag(String nodeType, String nodeId, String configJson) throws Exception {
        String nodes = source("s1", 1, "ops", OPS_FIELDS) + ","
                + "{\"id\":\"" + nodeId + "\",\"type\":\"" + nodeType
                + "\",\"data\":{\"config\":" + configJson + "}}";
        return dag(nodesEdges(nodes, edge("e1", "s1", nodeId, "in-0")));
    }

    @Test
    void replaceNullBuildsStatCteAndOutputColumns() throws Exception {
        SqlBuildResult result = factory.build(opsDag("replace_null", "r1", "{"
                + "\"alias\":\"a\",\"rules\":["
                + "{\"field\":\"amount\",\"strategy\":\"MAX\"},"
                + "{\"field\":\"city\",\"strategy\":\"VALUE\",\"value\":\"unknown\","
                + "\"output\":\"city_filled\"},"
                + "{\"field\":\"note\",\"strategy\":\"VALUE\",\"value\":\"\","
                + "\"treatEmptyAsNull\":true},"
                + "{\"field\":\"uid\",\"strategy\":\"AVG\"}"
                + "]}"), 101L, "r1");
        String sql = result.getSql();

        assertTrue(sql.startsWith("WITH dm_stat AS ("), sql);
        assertTrue(sql.contains("MAX(`amount`) AS `dm_max_amount`"), sql);
        assertTrue(sql.contains("AVG(`uid`) AS `dm_avg_uid`"), sql);
        assertTrue(sql.contains("CAST(COALESCE(`a`.`amount`, `dm_stat`.`dm_max_amount`) "
                + "AS DECIMAL(18,2)) AS `amount`"), sql);
        assertTrue(sql.contains("CAST(COALESCE(`a`.`uid`, `dm_stat`.`dm_avg_uid`) "
                + "AS DOUBLE) AS `uid`"), sql);
        assertTrue(sql.contains("COALESCE(`a`.`city`, 'unknown') AS `city_filled`"), sql);
        // treatEmptyAsNull 已废弃：旧配置不报错，也不再生成 NULLIF。
        assertTrue(sql.contains("COALESCE(`a`.`note`, '') AS `note`"), sql);
        assertTrue(!sql.contains("NULLIF"), sql);
        assertTrue(!sql.contains("(SELECT MAX("), sql);
        assertTrue(sql.contains("CROSS JOIN `dm_stat`"), sql);
        assertNoSourceTypes(sql);

        assertEquals(7, result.getOutputColumns().size());
        assertEquals("amount", result.getOutputColumns().get(1).getName());
        assertEquals("DECIMAL(18,2)", result.getOutputColumns().get(1).getDorisType());
        assertEquals("uid", result.getOutputColumns().get(0).getName());
        assertEquals("DOUBLE", result.getOutputColumns().get(0).getDorisType());
        assertEquals("city_filled", result.getOutputColumns().get(6).getName());
        assertEquals("VARCHAR(64)", result.getOutputColumns().get(6).getDorisType());
    }

    @Test
    void replaceNullValueOnlyHasNoCte() throws Exception {
        SqlBuildResult result = factory.build(opsDag("replace_null", "r1", "{"
                + "\"alias\":\"a\",\"rules\":["
                + "{\"field\":\"city\",\"strategy\":\"VALUE\",\"value\":\"unknown\"},"
                + "{\"field\":\"note\",\"strategy\":\"VALUE\",\"value\":\"\"}"
                + "]}"), 101L, "r1");
        String sql = result.getSql();
        assertTrue(!sql.startsWith("WITH"), sql);
        assertTrue(!sql.contains("CROSS JOIN"), sql);
        assertTrue(sql.contains("COALESCE(`a`.`city`, 'unknown') AS `city`"), sql);
        assertTrue(sql.contains("COALESCE(`a`.`note`, '') AS `note`"), sql);
        assertTrue(!sql.contains("NULLIF"), sql);
    }

    @Test
    void replaceNullSupportsMedianAndMode() throws Exception {
        SqlBuildResult result = factory.build(opsDag("replace_null", "r1", "{"
                + "\"alias\":\"a\",\"rules\":["
                + "{\"field\":\"amount\",\"strategy\":\"MEDIAN\"},"
                + "{\"field\":\"uid\",\"strategy\":\"MODE\"}"
                + "]}"), 101L, "r1");
        String sql = result.getSql();
        assertTrue(sql.contains("PERCENTILE(`amount`, 0.5) AS `dm_median_amount`"), sql);
        assertTrue(sql.contains("ORDER BY COUNT(*) DESC, `uid` ASC LIMIT 1"), sql);
        assertEquals("DOUBLE", result.getOutputColumns().get(1).getDorisType());
        assertEquals("BIGINT", result.getOutputColumns().get(0).getDorisType());
    }

    @Test
    void replaceNullModeOnlyCteHasNoOuterFrom() throws Exception {
        SqlBuildResult result = factory.build(opsDag("replace_null", "r1", "{"
                + "\"alias\":\"a\",\"rules\":[{\"field\":\"uid\",\"strategy\":\"MODE\"}]}"),
                101L, "r1");
        String sql = result.getSql();
        String cte = cteOf(sql);
        // 全为 MODE 时外层不带 FROM：标量子查询常量子查询，CTE 恒为 1 行，CROSS JOIN 不会放大。
        assertTrue(cte.contains("SELECT (SELECT `uid` FROM "), cte);
        assertFalse(cte.contains("\n  FROM "), cte);
        assertTrue(sql.contains("CROSS JOIN `dm_stat`"), sql);
        assertTrue(sql.contains("CAST(COALESCE(`a`.`uid`, `dm_stat`.`dm_mode_uid`) AS BIGINT)"), sql);
    }

    @Test
    void replaceNullMultipleModesCteHasNoOuterFrom() throws Exception {
        SqlBuildResult result = factory.build(opsDag("replace_null", "r1", "{"
                + "\"alias\":\"a\",\"rules\":[{\"field\":\"uid\",\"strategy\":\"MODE\"},"
                + "{\"field\":\"amount\",\"strategy\":\"MODE\"}]}"), 101L, "r1");
        String sql = result.getSql();
        String cte = cteOf(sql);
        assertTrue(cte.contains("AS `dm_mode_uid`"), cte);
        assertTrue(cte.contains("AS `dm_mode_amount`"), cte);
        assertFalse(cte.contains("\n  FROM "), cte);
    }

    @Test
    void replaceNullModeWithAggregateCteStaysSingleRow() throws Exception {
        SqlBuildResult result = factory.build(opsDag("replace_null", "r1", "{"
                + "\"alias\":\"a\",\"rules\":[{\"field\":\"uid\",\"strategy\":\"MODE\"},"
                + "{\"field\":\"amount\",\"strategy\":\"MAX\"}]}"), 101L, "r1");
        String sql = result.getSql();
        String cte = cteOf(sql);
        // 含聚合函数且无 GROUP BY 的 SELECT 恒为 1 行；MODE 子查询仍带自己的分组。
        assertTrue(cte.contains("\n  FROM dm_src_"), cte);
        assertTrue(cte.contains("MAX(`amount`) AS `dm_max_amount`"), cte);
        assertTrue(cte.contains("AS `dm_mode_uid`"), cte);
        assertFalse(hasTopLevelGroupBy(cte), cte);
    }

    private static String cteOf(String sql) {
        int end = sql.indexOf(")\nSELECT");
        assertTrue(end > 0, sql);
        return sql.substring(0, end);
    }

    /** CTE 内层（一层括号，深度 1）是否出现 GROUP BY；有则 CTE 可能返回多行。 */
    private static boolean hasTopLevelGroupBy(String sql) {
        int depth = 0;
        boolean inTick = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '`') {
                inTick = !inTick;
                continue;
            }
            if (inTick) {
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 1 && sql.regionMatches(true, i, "GROUP BY", 0, 8)) {
                return true;
            }
        }
        return false;
    }

    @Test
    void replaceNullRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"rules\":[]}", "REPLACE_RULES_EMPTY"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"city\",\"strategy\":\"MAX\"}]}",
                        "REPLACE_STRATEGY_UNSUPPORTED"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"nope\",\"strategy\":\"VALUE\","
                        + "\"value\":\"1\"}]}", "REPLACE_FIELD_UNKNOWN"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"amount\",\"strategy\":\"VALUE\","
                        + "\"value\":\"abc\"}]}", "REPLACE_VALUE_INVALID"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"amount\",\"strategy\":\"VALUE\"}]}",
                        "REPLACE_VALUE_MISSING"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"amount\",\"strategy\":\"MAX\"},"
                        + "{\"field\":\"amount\",\"strategy\":\"MIN\"}]}",
                        "REPLACE_RULE_DUPLICATE"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"amount\",\"strategy\":\"VALUE\","
                        + "\"value\":\"1\",\"output\":\"city\"}]}", "REPLACE_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"city\",\"strategy\":\"SUM\"}]}",
                        "REPLACE_STRATEGY_INVALID"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("replace_null", "r1", c[0]), 101L, "r1"), c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // -------------------------------------------------------- SortRows

    @Test
    void sortRowsBuildsSequenceSimpleAndGroupWindow() throws Exception {
        SqlBuildResult result = factory.build(opsDag("sort_rows", "o1", "{"
                + "\"alias\":\"a\",\"sequenceEnabled\":true,\"sequenceOutput\":\"seq_no\","
                + "\"simpleEnabled\":true,\"keys\":["
                + "{\"field\":\"city\",\"direction\":\"ASC\"},"
                + "{\"field\":\"amount\",\"direction\":\"CUSTOM\",\"values\":[\"10\",\"20\"]}],"
                + "\"groupEnabled\":true,\"partitionBy\":[\"uid\"],"
                + "\"groupOrderBy\":[{\"field\":\"amount\",\"direction\":\"DESC\"}],"
                + "\"groupFunction\":\"RANK\",\"groupOutput\":\"rn\"}"), 101L, "o1");
        String sql = result.getSql();

        String customOrder = "(CASE WHEN `a`.`amount` IS NULL OR "
                + "FIELD(`a`.`amount`, 10, 20) = 0 THEN 1 ELSE 0 END)";
        assertTrue(sql.contains("ROW_NUMBER() OVER (ORDER BY `a`.`city` ASC, "
                + customOrder + ", FIELD(`a`.`amount`, 10, 20) ASC) "
                + "AS `seq_no`"), sql);
        assertTrue(sql.contains("RANK() OVER (PARTITION BY `a`.`uid` ORDER BY "
                + "`a`.`amount` DESC) AS `rn`"), sql);
        assertTrue(sql.contains("ORDER BY `a`.`city` ASC, "
                + customOrder + ", FIELD(`a`.`amount`, 10, 20) ASC"), sql);
        assertEquals(8, result.getOutputColumns().size());
        assertEquals("seq_no", result.getOutputColumns().get(6).getName());
        assertEquals("BIGINT", result.getOutputColumns().get(6).getDorisType());
        assertEquals("rn", result.getOutputColumns().get(7).getName());
        assertEquals("BIGINT", result.getOutputColumns().get(7).getDorisType());
    }

    @Test
    void sortRowsGroupOnlyOrdersByPartitionAndGroupKeys() throws Exception {
        SqlBuildResult result = factory.build(opsDag("sort_rows", "o1", "{"
                + "\"alias\":\"a\",\"sequenceEnabled\":true,\"sequenceOutput\":\"seq_no\","
                + "\"groupEnabled\":true,\"partitionBy\":[\"uid\"],"
                + "\"groupOrderBy\":[{\"field\":\"city\",\"direction\":\"ASC\"}],"
                + "\"groupFunction\":\"DENSE_RANK\",\"groupOutput\":\"dr\"}"), 101L, "o1");
        String sql = result.getSql();
        assertTrue(sql.contains("DENSE_RANK() OVER (PARTITION BY `a`.`uid` ORDER BY "
                + "`a`.`city` ASC) AS `dr`"), sql);
        assertTrue(sql.contains("ROW_NUMBER() OVER (ORDER BY `a`.`uid` ASC, "
                + "`a`.`city` ASC) AS `seq_no`"), sql);
        assertTrue(sql.endsWith("ORDER BY `a`.`uid` ASC, `a`.`city` ASC"), sql);
    }

    @Test
    void sortRowsAllOffIsPassthrough() throws Exception {
        SqlBuildResult result = factory.build(opsDag("sort_rows", "o1", "{"
                + "\"alias\":\"a\",\"sequenceEnabled\":true,\"sequenceOutput\":\"seq_no\","
                + "\"simpleEnabled\":false,\"groupEnabled\":false}"), 101L, "o1");
        String sql = result.getSql();
        assertTrue(!sql.contains("OVER ("), sql);
        assertTrue(!sql.contains("ORDER BY"), sql);
        assertTrue(!sql.contains("`seq_no`"), sql);
        assertEquals(6, result.getOutputColumns().size());
    }

    @Test
    void sortRowsCustomStringValuesAreEscaped() throws Exception {
        SqlBuildResult result = factory.build(opsDag("sort_rows", "o1", "{"
                + "\"alias\":\"a\",\"simpleEnabled\":true,\"keys\":["
                + "{\"field\":\"city\",\"direction\":\"CUSTOM\",\"values\":[\"O'Hare\",\"B\"]}]}"),
                101L, "o1");
        assertTrue(result.getSql().contains(
                "FIELD(`a`.`city`, 'O''Hare', 'B')"), result.getSql());
    }

    @Test
    void sortRowsCustomSortPutsNullWithUnlistedValuesLast() throws Exception {
        SqlBuildResult result = factory.build(opsDag("sort_rows", "o1", "{"
                + "\"alias\":\"a\",\"simpleEnabled\":true,\"keys\":["
                + "{\"field\":\"city\",\"direction\":\"CUSTOM\",\"values\":[\"NEW\",\"PAID\"]}]}"),
                101L, "o1");
        String sql = result.getSql();
        assertTrue(sql.contains("ORDER BY (CASE WHEN `a`.`city` IS NULL OR "
                + "FIELD(`a`.`city`, 'NEW', 'PAID') = 0 THEN 1 ELSE 0 END), "
                + "FIELD(`a`.`city`, 'NEW', 'PAID') ASC"), sql);
        assertTrue(!sql.contains("(FIELD(`a`.`city`, 'NEW', 'PAID') = 0)"), sql);
    }

    @Test
    void sortRowsCustomNumericSortAlsoGuardsNull() throws Exception {
        SqlBuildResult result = factory.build(opsDag("sort_rows", "o1", "{"
                + "\"alias\":\"a\",\"sequenceEnabled\":true,\"sequenceOutput\":\"seq\","
                + "\"simpleEnabled\":true,\"keys\":["
                + "{\"field\":\"amount\",\"direction\":\"CUSTOM\",\"values\":[\"10\",\"20\"]}]}"),
                101L, "o1");
        assertTrue(result.getSql().contains("ROW_NUMBER() OVER (ORDER BY (CASE WHEN "
                + "`a`.`amount` IS NULL OR FIELD(`a`.`amount`, 10, 20) = 0 THEN 1 ELSE 0 END), "
                + "FIELD(`a`.`amount`, 10, 20) ASC) AS `seq`"), result.getSql());
    }

    @Test
    void sortRowsRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"simpleEnabled\":true,\"keys\":[]}", "SORT_KEYS_EMPTY"},
                {"{\"alias\":\"a\",\"simpleEnabled\":true,\"keys\":[{\"field\":\"nope\","
                        + "\"direction\":\"ASC\"}]}", "SORT_FIELD_UNKNOWN"},
                {"{\"alias\":\"a\",\"simpleEnabled\":true,\"keys\":[{\"field\":\"city\","
                        + "\"direction\":\"UP\"}]}", "SORT_DIRECTION_INVALID"},
                {"{\"alias\":\"a\",\"simpleEnabled\":true,\"keys\":[{\"field\":\"city\","
                        + "\"direction\":\"CUSTOM\",\"values\":[]}]}", "SORT_VALUES_EMPTY"},
                {"{\"alias\":\"a\",\"simpleEnabled\":true,\"keys\":[{\"field\":\"amount\","
                        + "\"direction\":\"CUSTOM\",\"values\":[\"x\"]}]}", "SORT_VALUE_INVALID"},
                {"{\"alias\":\"a\",\"groupEnabled\":true,\"partitionBy\":[],"
                        + "\"groupOrderBy\":[{\"field\":\"city\",\"direction\":\"ASC\"}],"
                        + "\"groupFunction\":\"RANK\",\"groupOutput\":\"rn\"}",
                        "SORT_PARTITION_EMPTY"},
                {"{\"alias\":\"a\",\"groupEnabled\":true,\"partitionBy\":[\"uid\"],"
                        + "\"groupOrderBy\":[],\"groupFunction\":\"RANK\",\"groupOutput\":\"rn\"}",
                        "SORT_GROUP_ORDER_EMPTY"},
                {"{\"alias\":\"a\",\"groupEnabled\":true,\"partitionBy\":[\"uid\"],"
                        + "\"groupOrderBy\":[{\"field\":\"city\",\"direction\":\"ASC\"}],"
                        + "\"groupFunction\":\"NTILE\",\"groupOutput\":\"rn\"}",
                        "SORT_FUNCTION_INVALID"},
                {"{\"alias\":\"a\",\"groupEnabled\":true,\"partitionBy\":[\"uid\"],"
                        + "\"groupOrderBy\":[{\"field\":\"city\",\"direction\":\"ASC\"}],"
                        + "\"groupFunction\":\"RANK\",\"groupOutput\":\"uid\"}",
                        "SORT_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"sequenceEnabled\":true,\"sequenceOutput\":\"uid\","
                        + "\"simpleEnabled\":true,\"keys\":[{\"field\":\"city\","
                        + "\"direction\":\"ASC\"}]}", "SORT_OUTPUT_DUPLICATE"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("sort_rows", "o1", c[0]), 101L, "o1"), c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // ----------------------------------------------------- SplitFields

    @Test
    void splitFieldsBuildsPartsAndOutputColumns() throws Exception {
        SqlBuildResult result = factory.build(opsDag("split_fields", "p1", "{"
                + "\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"/\","
                + "\"keepOriginal\":true,\"trim\":true,\"emptyAsNull\":true,"
                + "\"parts\":[{\"index\":1,\"output\":\"p1\"},{\"index\":2,\"output\":\"p2\"}]}"),
                101L, "p1");
        String sql = result.getSql();
        assertTrue(sql.contains("NULLIF(TRIM((CASE WHEN LOCATE('/', `a`.`path`) = 0 THEN "
                + "`a`.`path` ELSE SPLIT_PART(`a`.`path`, '/', 1) END)), '') AS `p1`"), sql);
        assertTrue(sql.contains("NULLIF(TRIM(SPLIT_PART(`a`.`path`, '/', 2)), '') AS `p2`"), sql);
        assertEquals(8, result.getOutputColumns().size());
        assertEquals("p1", result.getOutputColumns().get(6).getName());
        assertEquals("STRING", result.getOutputColumns().get(6).getDorisType());
        assertEquals("path", result.getOutputColumns().get(5).getName());
    }

    @Test
    void splitFieldsCanRemoveOriginalAndKeepEmptyStrings() throws Exception {
        SqlBuildResult result = factory.build(opsDag("split_fields", "p1", "{"
                + "\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"||\","
                + "\"keepOriginal\":false,\"trim\":false,\"emptyAsNull\":false,"
                + "\"parts\":[{\"index\":1,\"output\":\"head\"}]}"), 101L, "p1");
        String sql = result.getSql();
        assertTrue(sql.startsWith("SELECT\n  `a`.`uid`,"), sql);
        assertTrue(sql.contains("CASE WHEN `a`.`path` IS NULL THEN NULL ELSE "
                + "IFNULL((CASE WHEN LOCATE('||', `a`.`path`) = 0 THEN `a`.`path` "
                + "ELSE SPLIT_PART(`a`.`path`, '||', 1) END), '') END AS `head`"), sql);
        assertEquals(6, result.getOutputColumns().size());
        assertTrue(result.getOutputColumns().stream()
                .noneMatch(c -> c.getName().equals("path")));
    }

    @Test
    void splitFieldsFirstPartFallsBackToOriginalWithoutDelimiter() throws Exception {
        SqlBuildResult result = factory.build(opsDag("split_fields", "p1", "{"
                + "\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"/\","
                + "\"keepOriginal\":true,\"trim\":false,\"emptyAsNull\":true,"
                + "\"parts\":[{\"index\":1,\"output\":\"p1\"},"
                + "{\"index\":2,\"output\":\"p2\"}]}"), 101L, "p1");
        String sql = result.getSql();
        assertTrue(sql.contains("NULLIF((CASE WHEN LOCATE('/', `a`.`path`) = 0 THEN "
                + "`a`.`path` ELSE SPLIT_PART(`a`.`path`, '/', 1) END), '') AS `p1`"), sql);
        assertTrue(sql.contains("NULLIF(SPLIT_PART(`a`.`path`, '/', 2), '') AS `p2`"), sql);
        assertEquals(sql.indexOf("LOCATE("), sql.lastIndexOf("LOCATE("), sql);
    }

    @Test
    void splitFieldsNullSourceGuardRemainsWithDelimiterFallback() throws Exception {
        SqlBuildResult result = factory.build(opsDag("split_fields", "p1", "{"
                + "\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"||\","
                + "\"keepOriginal\":true,\"trim\":false,\"emptyAsNull\":false,"
                + "\"parts\":[{\"index\":1,\"output\":\"p1\"}]}"), 101L, "p1");
        assertTrue(result.getSql().contains("CASE WHEN `a`.`path` IS NULL THEN NULL ELSE "
                + "IFNULL((CASE WHEN LOCATE('||', `a`.`path`) = 0 THEN `a`.`path` "
                + "ELSE SPLIT_PART(`a`.`path`, '||', 1) END), '') END AS `p1`"),
                result.getSql());
    }

    @Test
    void splitFieldsRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"\","
                        + "\"parts\":[{\"index\":1,\"output\":\"p1\"}]}",
                        "SPLIT_DELIMITER_EMPTY"},
                {"{\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"/\",\"parts\":[]}",
                        "SPLIT_PARTS_EMPTY"},
                {"{\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"/\","
                        + "\"parts\":[{\"index\":0,\"output\":\"p1\"}]}", "SPLIT_INDEX_INVALID"},
                {"{\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"/\","
                        + "\"parts\":[{\"index\":1,\"output\":\"p1\"},"
                        + "{\"index\":1,\"output\":\"p2\"}]}", "SPLIT_INDEX_DUPLICATE"},
                {"{\"alias\":\"a\",\"field\":\"path\",\"delimiter\":\"/\","
                        + "\"parts\":[{\"index\":1,\"output\":\"city\"}]}",
                        "SPLIT_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"field\":\"nope\",\"delimiter\":\"/\","
                        + "\"parts\":[{\"index\":1,\"output\":\"p1\"}]}", "SPLIT_FIELD_UNKNOWN"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("split_fields", "p1", c[0]), 101L, "p1"), c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // --------------------------------------------- SplitFieldsToRows

    @Test
    void splitRowsBuildsOuterExplodeAndPosition() throws Exception {
        SqlBuildResult result = factory.build(opsDag("split_fields_to_rows", "x1", "{"
                + "\"alias\":\"a\",\"field\":\"tags\",\"delimiter\":\",\","
                + "\"output\":\"tag\",\"keepOriginal\":true,\"trim\":true,"
                + "\"dropEmpty\":false,\"positionOutput\":\"tag_index\"}"), 101L, "x1");
        String sql = result.getSql();
        assertTrue(sql.contains("LATERAL VIEW posexplode(IF(`a`.`tags` IS NULL OR "
                + "`a`.`tags` = '', ARRAY(NULL), SPLIT_BY_STRING(`a`.`tags`, ',')))"), sql);
        assertTrue(sql.contains("TRIM(`__dm_val`) AS `tag`"), sql);
        assertTrue(sql.contains("(`__dm_pos` + 1) AS `tag_index`"), sql);
        assertEquals(8, result.getOutputColumns().size());
        assertEquals("tag", result.getOutputColumns().get(6).getName());
        assertEquals("STRING", result.getOutputColumns().get(6).getDorisType());
        assertEquals("tag_index", result.getOutputColumns().get(7).getName());
        assertEquals("BIGINT", result.getOutputColumns().get(7).getDorisType());
    }

    @Test
    void splitRowsSupportsInPlaceAndDropEmpty() throws Exception {
        SqlBuildResult result = factory.build(opsDag("split_fields_to_rows", "x1", "{"
                + "\"alias\":\"a\",\"field\":\"tags\",\"delimiter\":\",\","
                + "\"output\":\"tags\",\"keepOriginal\":true,\"trim\":true,"
                + "\"dropEmpty\":true}"), 101L, "x1");
        String sql = result.getSql();
        assertTrue(sql.contains("TRIM(`__dm_val`) AS `tags`"), sql);
        assertTrue(!sql.contains("`a`.`tags`,\n"), sql);
        assertTrue(sql.contains("WHERE TRIM(`__dm_val`) IS NOT NULL AND "
                + "TRIM(`__dm_val`) <> ''"), sql);
        assertEquals(6, result.getOutputColumns().size());
        assertEquals("STRING", result.getOutputColumns().get(4).getDorisType());
    }

    @Test
    void splitRowsRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"field\":\"tags\",\"delimiter\":\"\","
                        + "\"output\":\"tag\"}", "SPLIT_DELIMITER_EMPTY"},
                {"{\"alias\":\"a\",\"field\":\"tags\",\"delimiter\":\",\","
                        + "\"output\":\"\"}", "SPLIT_OUTPUT_EMPTY"},
                {"{\"alias\":\"a\",\"field\":\"tags\",\"delimiter\":\",\","
                        + "\"output\":\"uid\"}", "SPLIT_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"field\":\"tags\",\"delimiter\":\",\","
                        + "\"output\":\"tag\",\"positionOutput\":\"tag\"}",
                        "SPLIT_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"field\":\"nope\",\"delimiter\":\",\","
                        + "\"output\":\"tag\"}", "SPLIT_FIELD_UNKNOWN"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("split_fields_to_rows", "x1", c[0]), 101L, "x1"),
                    c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // ------------------------------------------------------ ValueMapper

    @Test
    void valueMapperBuildsCaseInPlaceAndAppendedColumns() throws Exception {
        SqlBuildResult result = factory.build(opsDag("value_mapper", "vm1", "{"
                + "\"alias\":\"a\",\"rules\":["
                + "{\"field\":\"city\",\"mappings\":[{\"from\":\"N\",\"to\":\"NEW\"},"
                + "{\"from\":\"P\",\"to\":\"PAID\"}],\"unmatched\":\"KEEP\"},"
                + "{\"field\":\"uid\",\"output\":\"uid_level\","
                + "\"mappings\":[{\"from\":\"1\",\"to\":\"10\"}],"
                + "\"unmatched\":\"VALUE\",\"unmatchedValue\":\"0\"},"
                + "{\"field\":\"note\",\"mappings\":[{\"from\":\"x\",\"to\":\"y\"}],"
                + "\"unmatched\":\"NULL\"}"
                + "]}"), 101L, "vm1");
        String sql = result.getSql();

        assertTrue(sql.contains("CAST(CASE WHEN `a`.`city` = 'N' THEN 'NEW' "
                + "WHEN `a`.`city` = 'P' THEN 'PAID' ELSE `a`.`city` END AS VARCHAR(64)) "
                + "AS `city`"), sql);
        assertTrue(sql.contains("CAST(CASE WHEN `a`.`uid` = 1 THEN 10 ELSE 0 END "
                + "AS BIGINT) AS `uid_level`"), sql);
        assertTrue(sql.contains("CASE WHEN `a`.`note` = 'x' THEN 'y' ELSE NULL END"), sql);
        assertNoSourceTypes(sql);

        assertEquals(7, result.getOutputColumns().size());
        assertEquals("uid_level", result.getOutputColumns().get(6).getName());
        assertEquals("BIGINT", result.getOutputColumns().get(6).getDorisType());
    }

    @Test
    void valueMapperRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"rules\":[]}", "VALUE_MAPPER_RULES_EMPTY"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"nope\","
                        + "\"mappings\":[{\"from\":\"1\",\"to\":\"2\"}]}]}",
                        "VALUE_MAPPER_FIELD_UNKNOWN"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"note\",\"mappings\":[]}]}",
                        "VALUE_MAPPER_MAPPINGS_EMPTY"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"note\",\"mappings\":["
                        + "{\"from\":\"\",\"to\":\"x\"}]}]}", "VALUE_MAPPER_FROM_MISSING"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"note\",\"mappings\":["
                        + "{\"from\":\"a\",\"to\":\"x\"},{\"from\":\"a\",\"to\":\"y\"}]}]}",
                        "VALUE_MAPPER_FROM_DUPLICATE"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"uid\",\"mappings\":["
                        + "{\"from\":\"abc\",\"to\":\"2\"}]}]}",
                        "VALUE_MAPPER_MAPPING_NOT_NUMERIC"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"city\",\"output\":\"note\","
                        + "\"mappings\":[{\"from\":\"a\",\"to\":\"b\"}]}]}",
                        "VALUE_MAPPER_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"city\",\"unmatched\":\"DROP\","
                        + "\"mappings\":[{\"from\":\"a\",\"to\":\"b\"}]}]}",
                        "VALUE_MAPPER_UNMATCHED_INVALID"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"city\",\"unmatched\":\"VALUE\","
                        + "\"mappings\":[{\"from\":\"a\",\"to\":\"b\"}]}]}",
                        "VALUE_MAPPER_UNMATCHED_VALUE_MISSING"},
                {"{\"alias\":\"a\",\"rules\":[{\"field\":\"city\",\"mappings\":["
                        + "{\"from\":\"a\",\"to\":\"b\"}]},{\"field\":\"city\",\"mappings\":["
                        + "{\"from\":\"c\",\"to\":\"d\"}]}]}", "VALUE_MAPPER_FIELD_DUPLICATE"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("value_mapper", "vm1", c[0]), 101L, "vm1"), c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // ------------------------------------------------------- UniqueRows

    @Test
    void uniqueRowsSimpleUsesRowNumberOverKeys() throws Exception {
        SqlBuildResult result = factory.build(opsDag("unique_rows", "u1", "{"
                + "\"alias\":\"a\",\"mode\":\"SIMPLE\",\"keyFields\":[\"uid\",\"city\"],"
                + "\"duplicateOutput\":\"DISCARD\"}"), 101L, "u1");
        String sql = result.getSql();
        assertTrue(sql.contains("ROW_NUMBER() OVER (PARTITION BY `a`.`uid`, `a`.`city` "
                + "ORDER BY RAND()) AS `dm_rn`"), sql);
        assertTrue(sql.contains("WHERE `t`.`dm_rn` = 1"), sql);
        assertTrue(!sql.contains("`a`.*"), sql);
        assertEquals(6, result.getOutputColumns().size());
        assertEquals("uid", result.getOutputColumns().get(0).getName());
    }

    @Test
    void uniqueRowsRuleHonoursOrderAndIgnoreCaseOnlyForStrings() throws Exception {
        SqlBuildResult result = factory.build(opsDag("unique_rows", "u1", "{"
                + "\"alias\":\"a\",\"mode\":\"RULE\",\"keyFields\":[\"city\"],"
                + "\"ignoreCase\":true,\"orderBy\":[{\"field\":\"amount\","
                + "\"direction\":\"DESC\"}],\"duplicateOutput\":\"DISCARD\"}"), 101L, "u1");
        String sql = result.getSql();
        assertTrue(sql.contains("PARTITION BY LOWER(CAST(`a`.`city` AS STRING))"), sql);
        assertTrue(sql.contains("ORDER BY `a`.`amount` DESC"), sql);

        SqlBuildResult numeric = factory.build(opsDag("unique_rows", "u1", "{"
                + "\"alias\":\"a\",\"keyFields\":[\"uid\"],\"ignoreCase\":true,"
                + "\"duplicateOutput\":\"DISCARD\"}"), 101L, "u1");
        assertTrue(numeric.getSql().contains("PARTITION BY `a`.`uid`"), numeric.getSql());
        assertTrue(!numeric.getSql().contains("LOWER(`a`.`uid`)"), numeric.getSql());
    }

    @Test
    void uniqueRowsDistinctOptimizationOnlyWhenAllColumnsAreKeys() throws Exception {
        SqlBuildResult result = factory.build(opsDag("unique_rows", "u1", "{"
                + "\"alias\":\"a\",\"keyFields\":[\"uid\",\"amount\",\"city\",\"note\","
                + "\"tags\",\"path\"],\"duplicateOutput\":\"DISCARD\"}"), 101L, "u1");
        assertTrue(result.getSql().startsWith("SELECT DISTINCT"), result.getSql());
        assertTrue(!result.getSql().contains("ROW_NUMBER()"), result.getSql());
        assertTrue(!result.getSql().contains("`t`."), result.getSql());
        assertTrue(result.getSql().contains(" AS `a`"), result.getSql());

        SqlBuildResult partial = factory.build(opsDag("unique_rows", "u1", "{"
                + "\"alias\":\"a\",\"keyFields\":[\"uid\",\"amount\",\"city\",\"note\","
                + "\"tags\"],\"duplicateOutput\":\"DISCARD\"}"), 101L, "u1");
        assertTrue(partial.getSql().contains("ROW_NUMBER()"), partial.getSql());
    }

    @Test
    void uniqueRowsDuplicateTableUsesSingleRowNumberMaterialization() throws Exception {
        String config = "{\"alias\":\"a\",\"keyFields\":[\"uid\",\"amount\",\"city\",\"note\","
                + "\"tags\",\"path\"],\"duplicateOutput\":\"TABLE\",\"duplicateTarget\":{"
                + "\"dataSourceId\":1,\"tableName\":\"ops_dup\",\"writeMode\":\"APPEND\","
                + "\"autoCreate\":true}}";
        JsonNode dagJson = opsDag("unique_rows", "u1", config);
        SqlBuildResult result = factory.build(dagJson, 101L, "u1");
        assertTrue(!result.getSql().startsWith("SELECT DISTINCT"), result.getSql());
        assertTrue(result.getSql().contains("ROW_NUMBER()"), result.getSql());

        SqlBuildResult numbered = factory.buildUniqueRowsNumbered(dagJson, 101L, "u1");
        assertTrue(numbered.getSql().contains("ROW_NUMBER() OVER (PARTITION BY `a`.`uid`, "
                + "`a`.`amount`, `a`.`city`, `a`.`note`, `a`.`tags`, `a`.`path` "
                + "ORDER BY RAND()) AS `dm_rn`"), numbered.getSql());
        assertFalse(numbered.getSql().contains("WHERE"), numbered.getSql());
        assertEquals(7, numbered.getOutputColumns().size());
        assertEquals("dm_rn", numbered.getOutputColumns().get(6).getName());
        assertEquals("BIGINT", numbered.getOutputColumns().get(6).getDorisType());

        SqlBuildResult main = factory.buildUniqueRowsFromNumbered(dagJson, 101L, "u1",
                "dm_mid_101_u1_rn", false);
        assertTrue(main.getSql().contains("FROM `dm_mid_101_u1_rn` AS `t`"), main.getSql());
        assertTrue(main.getSql().contains("WHERE `t`.`dm_rn` = 1"), main.getSql());
        assertFalse(main.getSql().contains("RAND()"), main.getSql());
        assertEquals(6, main.getOutputColumns().size());
        assertFalse(main.getOutputColumns().stream()
                .anyMatch(c -> "dm_rn".equalsIgnoreCase(c.getName())), main.getSql());

        SqlBuildResult dup = factory.buildUniqueRowsFromNumbered(dagJson, 101L, "u1",
                "dm_mid_101_u1_rn", true);
        assertTrue(dup.getSql().contains("WHERE `t`.`dm_rn` > 1"), dup.getSql());
        assertFalse(dup.getSql().contains("RAND()"), dup.getSql());
        assertEquals(6, dup.getOutputColumns().size());
        assertFalse(dup.getOutputColumns().stream()
                .anyMatch(c -> "dm_rn".equalsIgnoreCase(c.getName())), dup.getSql());
    }

    @Test
    void uniqueRowsRuleNumberedReusesDeterministicOrder() throws Exception {
        String config = "{\"alias\":\"a\",\"mode\":\"RULE\",\"keyFields\":[\"city\"],"
                + "\"orderBy\":[{\"field\":\"amount\",\"direction\":\"DESC\"}],"
                + "\"duplicateOutput\":\"TABLE\",\"duplicateTarget\":{"
                + "\"dataSourceId\":1,\"tableName\":\"ops_dup\",\"writeMode\":\"APPEND\"}}";
        JsonNode dagJson = opsDag("unique_rows", "u1", config);

        SqlBuildResult numbered = factory.buildUniqueRowsNumbered(dagJson, 101L, "u1");
        assertTrue(numbered.getSql().contains(
                "ORDER BY `a`.`amount` DESC) AS `dm_rn`"), numbered.getSql());
        assertFalse(numbered.getSql().contains("RAND()"), numbered.getSql());

        SqlBuildResult main = factory.buildUniqueRowsFromNumbered(dagJson, 101L, "u1",
                "dm_mid_101_u1_rn", false);
        SqlBuildResult dup = factory.buildUniqueRowsFromNumbered(dagJson, 101L, "u1",
                "dm_mid_101_u1_rn", true);
        assertTrue(main.getSql().contains("= 1"), main.getSql());
        assertTrue(dup.getSql().contains("> 1"), dup.getSql());
        assertFalse(main.getSql().contains("ROW_NUMBER"), main.getSql());
        assertFalse(dup.getSql().contains("ROW_NUMBER"), dup.getSql());
    }

    @Test
    void uniqueRowsRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"keyFields\":[]}", "UNIQUE_ROWS_KEYS_EMPTY"},
                {"{\"alias\":\"a\",\"mode\":\"FAST\",\"keyFields\":[\"uid\"]}",
                        "UNIQUE_ROWS_MODE_INVALID"},
                {"{\"alias\":\"a\",\"keyFields\":[\"nope\"]}", "UNIQUE_ROWS_FIELD_UNKNOWN"},
                {"{\"alias\":\"a\",\"mode\":\"RULE\",\"keyFields\":[\"uid\"]}",
                        "UNIQUE_ROWS_ORDER_EMPTY"},
                {"{\"alias\":\"a\",\"mode\":\"RULE\",\"keyFields\":[\"uid\"],"
                        + "\"orderBy\":[{\"field\":\"city\",\"direction\":\"UP\"}]}",
                        "SORT_DIRECTION_INVALID"},
                {"{\"alias\":\"a\",\"keyFields\":[\"uid\"],\"duplicateOutput\":\"DROP\"}",
                        "UNIQUE_ROWS_DUPLICATE_OUTPUT_INVALID"},
                {"{\"alias\":\"a\",\"keyFields\":[\"uid\"],\"duplicateOutput\":\"TABLE\"}",
                        "UNIQUE_ROWS_DUPLICATE_TARGET_MISSING"},
                {"{\"alias\":\"a\",\"keyFields\":[\"uid\"],\"duplicateOutput\":\"TABLE\","
                        + "\"duplicateTarget\":{\"tableName\":\"x\"}}",
                        "UNIQUE_ROWS_DUPLICATE_TARGET_INVALID"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("unique_rows", "u1", c[0]), 101L, "u1"), c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // --------------------------------------------------- ColumnsToRows

    @Test
    void columnsToRowsBuildsUnionAllAndOutputs() throws Exception {
        SqlBuildResult result = factory.build(opsDag("columns_to_rows", "p1", "{"
                + "\"alias\":\"a\",\"keepFields\":[\"uid\",\"city\"],"
                + "\"valueFields\":[{\"field\":\"amount\",\"label\":\"AMT\"},"
                + "{\"field\":\"uid\",\"label\":\"UID\"}],"
                + "\"nameOutput\":\"metric\",\"valueOutput\":\"val\","
                + "\"valueType\":\"STRING\",\"skipNull\":true}"), 101L, "p1");
        String sql = result.getSql();
        assertTrue(sql.contains("SELECT\n  `a`.`uid`,\n  `a`.`city`,\n  'AMT' AS `metric`,\n"
                + "  CAST(`a`.`amount` AS STRING) AS `val`\nFROM "), sql);
        assertTrue(sql.contains("WHERE `a`.`amount` IS NOT NULL"), sql);
        assertTrue(sql.contains("UNION ALL"), sql);
        assertTrue(sql.contains("'UID' AS `metric`"), sql);

        assertEquals(4, result.getOutputColumns().size());
        assertEquals("uid", result.getOutputColumns().get(0).getName());
        assertEquals("city", result.getOutputColumns().get(1).getName());
        assertEquals("metric", result.getOutputColumns().get(2).getName());
        assertEquals("STRING", result.getOutputColumns().get(2).getDorisType());
        assertEquals("val", result.getOutputColumns().get(3).getName());
        assertEquals("STRING", result.getOutputColumns().get(3).getDorisType());
    }

    @Test
    void columnsToRowsSupportsEmptyKeepAndDoubleValues() throws Exception {
        SqlBuildResult result = factory.build(opsDag("columns_to_rows", "p1", "{"
                + "\"alias\":\"a\",\"keepFields\":[],"
                + "\"valueFields\":[{\"field\":\"amount\",\"label\":\"A\"}],"
                + "\"valueType\":\"DOUBLE\"}"), 101L, "p1");
        assertTrue(result.getSql().contains("CAST(`a`.`amount` AS DOUBLE) AS `field_value`"),
                result.getSql());
        assertEquals(2, result.getOutputColumns().size());
        assertEquals("field_name", result.getOutputColumns().get(0).getName());
        assertEquals("field_value", result.getOutputColumns().get(1).getName());
        assertEquals("DOUBLE", result.getOutputColumns().get(1).getDorisType());
    }

    @Test
    void columnsToRowsRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"valueFields\":[]}",
                        "COLUMNS_TO_ROWS_VALUE_FIELDS_EMPTY"},
                {"{\"alias\":\"a\",\"valueFields\":[{\"field\":\"nope\"}]}",
                        "COLUMNS_TO_ROWS_FIELD_UNKNOWN"},
                {"{\"alias\":\"a\",\"keepFields\":[\"nope\"],"
                        + "\"valueFields\":[{\"field\":\"city\"}]}",
                        "COLUMNS_TO_ROWS_FIELD_UNKNOWN"},
                {"{\"alias\":\"a\",\"valueFields\":[{\"field\":\"city\"}],"
                        + "\"nameOutput\":\"x\",\"valueOutput\":\"x\"}",
                        "COLUMNS_TO_ROWS_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"keepFields\":[\"city\"],"
                        + "\"valueFields\":[{\"field\":\"amount\"}],"
                        + "\"nameOutput\":\"city\"}", "COLUMNS_TO_ROWS_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"valueFields\":[{\"field\":\"amount\"}],"
                        + "\"valueType\":\"INT\"}", "COLUMNS_TO_ROWS_VALUE_TYPE_INVALID"},
                {"{\"alias\":\"a\",\"valueFields\":[{\"field\":\"city\"}],"
                        + "\"valueType\":\"DOUBLE\"}",
                        "COLUMNS_TO_ROWS_VALUE_TYPE_UNSUPPORTED"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("columns_to_rows", "p1", c[0]), 101L, "p1"),
                    c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // --------------------------------------------------- RowsToColumns

    @Test
    void rowsToColumnsBuildsConditionalAggregates() throws Exception {
        SqlBuildResult result = factory.build(opsDag("rows_to_columns", "w1", "{"
                + "\"alias\":\"a\",\"groupFields\":[\"uid\",\"city\"],"
                + "\"pivotField\":\"note\",\"valueField\":\"amount\","
                + "\"aggregate\":\"SUM\",\"pivotValues\":["
                + "{\"value\":\"Q1\",\"output\":\"q1\"},"
                + "{\"value\":\"Q2\",\"output\":\"q2\"}]}"), 101L, "w1");
        String sql = result.getSql();
        assertTrue(sql.contains("SUM(CASE WHEN `a`.`note` = 'Q1' THEN `a`.`amount` END) "
                + "AS `q1`"), sql);
        assertTrue(sql.contains("SUM(CASE WHEN `a`.`note` = 'Q2' THEN `a`.`amount` END) "
                + "AS `q2`"), sql);
        assertTrue(sql.contains("GROUP BY `a`.`uid`, `a`.`city`"), sql);

        assertEquals(4, result.getOutputColumns().size());
        assertEquals("uid", result.getOutputColumns().get(0).getName());
        assertEquals("BIGINT", result.getOutputColumns().get(0).getDorisType());
        assertEquals("city", result.getOutputColumns().get(1).getName());
        assertEquals("VARCHAR(64)", result.getOutputColumns().get(1).getDorisType());
        assertEquals("q1", result.getOutputColumns().get(2).getName());
        assertEquals("DECIMAL(18,2)", result.getOutputColumns().get(2).getDorisType());
    }

    @Test
    void rowsToColumnsAggregateTypesAndNumericPivot() throws Exception {
        SqlBuildResult count = factory.build(opsDag("rows_to_columns", "w1", "{"
                + "\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"uid\","
                + "\"valueField\":\"amount\",\"aggregate\":\"COUNT\","
                + "\"pivotValues\":[{\"value\":\"1\",\"output\":\"u1\"}]}"), 101L, "w1");
        assertTrue(count.getSql().contains("COUNT(CASE WHEN `a`.`uid` = 1 THEN "
                + "`a`.`amount` END) AS `u1`"), count.getSql());
        assertEquals("BIGINT", count.getOutputColumns().get(1).getDorisType());

        SqlBuildResult avg = factory.build(opsDag("rows_to_columns", "w1", "{"
                + "\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"uid\","
                + "\"valueField\":\"amount\",\"aggregate\":\"AVG\","
                + "\"pivotValues\":[{\"value\":\"1\",\"output\":\"u1\"}]}"), 101L, "w1");
        assertEquals("DOUBLE", avg.getOutputColumns().get(1).getDorisType());
    }

    @Test
    void rowsToColumnsRejectsInvalidConfigurations() throws Exception {
        String[][] cases = {
                {"{\"alias\":\"a\",\"groupFields\":[],\"pivotField\":\"note\","
                        + "\"valueField\":\"amount\",\"aggregate\":\"SUM\","
                        + "\"pivotValues\":[{\"value\":\"Q1\",\"output\":\"q1\"}]}",
                        "ROWS_TO_COLUMNS_GROUP_EMPTY"},
                {"{\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"nope\","
                        + "\"valueField\":\"amount\",\"aggregate\":\"SUM\","
                        + "\"pivotValues\":[{\"value\":\"Q1\",\"output\":\"q1\"}]}",
                        "ROWS_TO_COLUMNS_PIVOT_FIELD_UNKNOWN"},
                {"{\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"note\","
                        + "\"valueField\":\"amount\",\"aggregate\":\"MEDIAN\","
                        + "\"pivotValues\":[{\"value\":\"Q1\",\"output\":\"q1\"}]}",
                        "ROWS_TO_COLUMNS_AGGREGATE_INVALID"},
                {"{\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"note\","
                        + "\"valueField\":\"amount\",\"aggregate\":\"SUM\",\"pivotValues\":[]}",
                        "ROWS_TO_COLUMNS_VALUES_EMPTY"},
                {"{\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"note\","
                        + "\"valueField\":\"amount\",\"aggregate\":\"SUM\","
                        + "\"pivotValues\":[{\"value\":\"Q1\",\"output\":\"\"}]}",
                        "ROWS_TO_COLUMNS_OUTPUT_EMPTY"},
                {"{\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"note\","
                        + "\"valueField\":\"amount\",\"aggregate\":\"SUM\","
                        + "\"pivotValues\":[{\"value\":\"Q1\",\"output\":\"q1\"},"
                        + "{\"value\":\"Q2\",\"output\":\"q1\"}]}",
                        "ROWS_TO_COLUMNS_OUTPUT_DUPLICATE"},
                {"{\"alias\":\"a\",\"groupFields\":[\"city\"],\"pivotField\":\"uid\","
                        + "\"valueField\":\"amount\",\"aggregate\":\"SUM\","
                        + "\"pivotValues\":[{\"value\":\"Q1\",\"output\":\"q1\"}]}",
                        "ROWS_TO_COLUMNS_VALUE_INVALID"},
        };
        for (String[] c : cases) {
            BizException e = assertThrows(BizException.class,
                    () -> factory.build(opsDag("rows_to_columns", "w1", c[0]), 101L, "w1"),
                    c[0]);
            assertEquals(4000, e.getErrorCode().getCode(), c[0]);
            assertEquals(c[1], reason(e), c[0] + " -> " + e.getMessage());
        }
    }

    // -------------------------------------------------- TypeNormalization

    @Test
    void sourceTypesNormalizedBeforeSqlAndMetadata() throws Exception {
        String fields = field("id", "bigint unsigned") + ","
                + field("small", "int unsigned") + ","
                + field("tiny", "tinyint unsigned") + ","
                + field("ts", "timestamp") + ","
                + field("amt", "decimal(18,2)");
        String nodes = source("s1", 4242, "leaky", fields) + ","
                + "{\"id\":\"vm1\",\"type\":\"value_mapper\",\"data\":{\"config\":{"
                + "\"alias\":\"a\",\"rules\":[{\"field\":\"id\",\"mappings\":["
                + "{\"from\":\"1\",\"to\":\"2\"}]}]}}}";
        SqlBuildResult result = factory.build(dag(nodesEdges(nodes,
                edge("e1", "s1", "vm1", "in-0"))), 101L, "vm1");
        assertNoSourceTypes(result.getSql());
        assertEquals("LARGEINT", result.getOutputColumns().get(0).getDorisType());
        assertEquals("BIGINT", result.getOutputColumns().get(1).getDorisType());
        assertEquals("SMALLINT", result.getOutputColumns().get(2).getDorisType());
        assertEquals("DATETIME", result.getOutputColumns().get(3).getDorisType());
        assertEquals("DECIMAL(18,2)", result.getOutputColumns().get(4).getDorisType());
    }

    private static void assertNoSourceTypes(String sql) {
        assertTrue(!java.util.regex.Pattern.compile("(?i)\\b(unsigned|zerofill|varchar2)\\b")
                .matcher(sql).find(), sql);
        assertTrue(!sql.toLowerCase().contains("number("), sql);
    }

    private static String repeat(String value, int times) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append(value);
        }
        return sb.toString();
    }
}
