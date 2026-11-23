package com.datamodel.sink;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.OutputColumn;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkConfigParserTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SinkConfigParser parser = new SinkConfigParser();

    private DagNode node(String config) throws Exception {
        String json = "{\"nodes\":[{\"id\":\"s\",\"type\":\"sink\",\"data\":{\"config\":"
                + config + "}}],\"edges\":[]}";
        return Dag.parse(mapper.readTree(json)).node("s");
    }

    @Test
    void parsesDefaultsAndFields() throws Exception {
        SinkConfig config = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"qa_sink_b37\","
                + "\"fieldMapping\":[{\"source\":\"id\",\"target\":\"id\",\"type\":\"BIGINT\"}]}"));

        assertEquals("DORIS", config.getTargetType());
        assertEquals("APPEND", config.getWriteMode());
        assertEquals(10000, config.getBatchSize());
        assertTrue(config.isAutoCreateTable());
        assertNull(config.getWatermarkColumn());
        assertEquals("qa_sink_b37", config.getTableName());
        assertEquals("id", config.getFieldMapping().get(0).getSource());
    }

    @Test
    void rejectsMissingTableNameWith4007() throws Exception {
        BizException e = assertThrows(BizException.class, () -> parser.parse(node("{}")));
        assertEquals(ErrorCode.SINK_CONFIG_INVALID, e.getErrorCode());
        assertEquals("请输入目标表名", e.getMessage());
        assertEquals("TABLE_NAME_REQUIRED", reason(e));
    }

    @Test
    void dorisTargetRequiresDataSource() throws Exception {
        BizException e = assertThrows(BizException.class,
                () -> parser.parse(node("{\"tableName\":\"t\"}")));
        assertEquals(ErrorCode.SINK_CONFIG_INVALID, e.getErrorCode());
        assertEquals("DATA_SOURCE_REQUIRED", reason(e));
        assertEquals("请选择 Doris 目标数据源（Stream Load 目标）", e.getMessage());
    }

    @Test
    void rejectsSchemaQualifiedTableName() throws Exception {
        BizException e = assertThrows(BizException.class,
                () -> parser.parse(node("{\"tableName\":\"db.t\"}")));
        assertEquals("TABLE_NAME_SCHEMA_UNSUPPORTED", reason(e));
    }

    @Test
    void rejectsUpsert() throws Exception {
        BizException e = assertThrows(BizException.class,
                () -> parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\",\"writeMode\":\"UPSERT\"}")));
        assertEquals("WRITE_MODE_UNSUPPORTED", reason(e));
    }

    @Test
    void rejectsBatchSizeOutOfRange() throws Exception {
        BizException low = assertThrows(BizException.class,
                () -> parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\",\"batchSize\":99}")));
        assertEquals("BATCH_SIZE_INVALID", reason(low));
        BizException high = assertThrows(BizException.class,
                () -> parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\",\"batchSize\":100001}")));
        assertEquals("BATCH_SIZE_INVALID", reason(high));
    }

    @Test
    void rejectsDuplicateTargetAndFillsDefaultsFromUpstream() throws Exception {
        SinkConfig duplicate = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\",\"fieldMapping\":["
                + "{\"source\":\"a\",\"target\":\"x\"},{\"source\":\"b\",\"target\":\"X\"}]}"));
        BizException e = assertThrows(BizException.class,
                () -> parser.resolve(duplicate, Arrays.asList(
                        new OutputColumn("a", "BIGINT"), new OutputColumn("b", "STRING"))));
        assertEquals("FIELD_TARGET_DUPLICATE", reason(e));

        SinkConfig empty = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\"}"));
        parser.resolve(empty, Arrays.asList(
                new OutputColumn("a", "BIGINT"), new OutputColumn("b", "STRING")));
        assertEquals(2, empty.getFieldMapping().size());
        assertEquals("a", empty.getFieldMapping().get(0).getSource());
    }

    @Test
    void watermarkMustExistInUpstream() throws Exception {
        SinkConfig config = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\",\"watermarkColumn\":\"missing\"}"));
        BizException e = assertThrows(BizException.class,
                () -> parser.resolve(config, Collections.singletonList(
                        new OutputColumn("id", "BIGINT"))));
        assertEquals("WATERMARK_MISSING", reason(e));
    }

    @Test
    void watermarkFallsBackToFieldMappingSourcesWhenUpstreamUnavailable() throws Exception {
        SinkConfig invalid = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\",\"watermarkColumn\":\"missing\","
                + "\"fieldMapping\":[{\"source\":\"id\",\"target\":\"id\"}]}"));
        BizException e = assertThrows(BizException.class,
                () -> parser.resolve(invalid, Collections.emptyList()));
        assertEquals("WATERMARK_MISSING", reason(e));

        SinkConfig valid = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\",\"watermarkColumn\":\"id\","
                + "\"fieldMapping\":[{\"source\":\"id\",\"target\":\"id\"}]}"));
        parser.resolve(valid, Collections.emptyList());
        assertEquals("id", valid.getWatermarkColumn());
    }

    @Test
    void missingTableModelDefaultsToDuplicate() throws Exception {
        SinkConfig config = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\","
                + "\"fieldMapping\":[{\"source\":\"id\",\"target\":\"id\"}]}"));

        assertEquals(SinkConfig.DUPLICATE, config.getTableModel());
        assertFalse(config.isTableModelExplicit());
        assertTrue(config.getKeyColumns().isEmpty());
    }

    @Test
    void rejectsUnknownTableModel() throws Exception {
        BizException e = assertThrows(BizException.class, () -> parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"tableModel\":\"AGGREGATE\"}")));
        assertEquals(ErrorCode.SINK_CONFIG_INVALID, e.getErrorCode());
        assertEquals("TABLE_MODEL_INVALID", reason(e));
        assertEquals("不支持的建表模型：AGGREGATE", e.getMessage());
    }

    @Test
    void uniqueRequiresAtLeastOneKey() throws Exception {
        BizException e = assertThrows(BizException.class, () -> parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"tableModel\":\"UNIQUE\"}")));
        assertEquals("KEY_COLUMNS_REQUIRED", reason(e));
        assertEquals("Unique Key 模型请选择至少一个主键字段", e.getMessage());
    }

    @Test
    void duplicateModelRejectsKeyColumns() throws Exception {
        BizException e = assertThrows(BizException.class, () -> parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"tableModel\":\"DUPLICATE\","
                        + "\"keyColumns\":[\"id\"]}")));
        assertEquals("KEY_COLUMNS_UNSUPPORTED", reason(e));
        assertEquals("Duplicate Key 模型不支持设置主键", e.getMessage());
    }

    @Test
    void explicitDuplicateRejectsOverwrite() throws Exception {
        BizException e = assertThrows(BizException.class, () -> parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"tableModel\":\"DUPLICATE\","
                        + "\"writeMode\":\"OVERWRITE\"}")));
        assertEquals("WRITE_MODE_UNSUPPORTED", reason(e));
        assertEquals("Duplicate Key 模型仅支持追加写入", e.getMessage());
    }

    @Test
    void legacyDagWithoutModelStillAllowsOverwrite() throws Exception {
        SinkConfig config = parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"writeMode\":\"OVERWRITE\"}"));
        assertEquals(SinkConfig.OVERWRITE, config.getWriteMode());
        assertEquals(SinkConfig.DUPLICATE, config.getTableModel());
        assertFalse(config.isTableModelExplicit());
    }

    @Test
    void uniqueAllowsOverwrite() throws Exception {
        SinkConfig config = parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"tableModel\":\"UNIQUE\","
                        + "\"keyColumns\":[\"id\"],\"writeMode\":\"OVERWRITE\"}"));
        assertTrue(config.isUniqueModel());
        assertEquals(Collections.singletonList("id"), config.getKeyColumns());
    }

    @Test
    void rejectsDuplicateAndHiddenKeys() throws Exception {
        BizException duplicate = assertThrows(BizException.class, () -> parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"tableModel\":\"UNIQUE\","
                        + "\"keyColumns\":[\"id\",\"ID\"]}")));
        assertEquals("KEY_COLUMN_DUPLICATE", reason(duplicate));
        assertEquals("主键字段重复：ID", duplicate.getMessage());

        BizException hidden = assertThrows(BizException.class, () -> parser.parse(node(
                "{\"dataSourceId\":1,\"tableName\":\"t\",\"tableModel\":\"UNIQUE\","
                        + "\"keyColumns\":[\"__dm_rowid\"]}")));
        assertEquals("KEY_COLUMN_UNKNOWN", reason(hidden));
        assertEquals("主键字段不在输出字段中：__dm_rowid", hidden.getMessage());
    }

    @Test
    void uniqueKeysMustBeTargetColumns() throws Exception {
        SinkConfig config = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\","
                + "\"tableModel\":\"UNIQUE\",\"keyColumns\":[\"missing\"],"
                + "\"fieldMapping\":[{\"source\":\"id\",\"target\":\"id\"}]}"));

        BizException e = assertThrows(BizException.class, () -> parser.resolve(config,
                Collections.singletonList(new OutputColumn("id", "BIGINT"))));
        assertEquals("KEY_COLUMN_UNKNOWN", reason(e));
    }

    @Test
    void uniqueKeysNormalizeToTargetCase() throws Exception {
        SinkConfig config = parser.parse(node("{\"dataSourceId\":1,\"tableName\":\"t\","
                + "\"tableModel\":\"UNIQUE\",\"keyColumns\":[\"ID\"],"
                + "\"fieldMapping\":[{\"source\":\"src_id\",\"target\":\"id\"}]}"));

        parser.resolve(config, Collections.singletonList(new OutputColumn("src_id", "BIGINT")));
        assertEquals(Collections.singletonList("id"), config.getKeyColumns());
    }

    @SuppressWarnings("unchecked")
    private static String reason(BizException e) {
        return (String) ((Map<String, Object>) e.getData()).get("reason");
    }
}
