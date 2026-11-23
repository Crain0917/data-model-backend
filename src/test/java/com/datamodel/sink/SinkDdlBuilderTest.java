package com.datamodel.sink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkDdlBuilderTest {

    private static SinkConfig config(String targetType) {
        SinkConfig config = new SinkConfig();
        config.setNodeId("sink-1");
        config.setTargetType(targetType);
        config.setTableName("qa_sink_b37");
        config.setFieldMapping(Arrays.asList(
                new SinkField("id", "id", "BIGINT"),
                new SinkField("name", "name", "VARCHAR(20)")));
        return config;
    }

    @Test
    void dorisDdlInjectsSequenceKeyFirst() {
        String ddl = SinkDdlBuilder.buildCreateTable(config("DORIS"), "`testdb`.`qa_sink_b37`");

        assertTrue(ddl.startsWith("CREATE TABLE IF NOT EXISTS `testdb`.`qa_sink_b37` ("));
        int seq = ddl.indexOf("`__dm_sink_seq` BIGINT NOT NULL");
        int id = ddl.indexOf("`id` BIGINT");
        assertTrue(seq > 0 && id > seq, "隐藏键列必须是首列");
        assertTrue(ddl.contains("DUPLICATE KEY(`__dm_sink_seq`)"));
        assertTrue(ddl.contains("DISTRIBUTED BY HASH(`__dm_sink_seq`) BUCKETS 1"));
        assertTrue(ddl.contains("PROPERTIES (\"replication_num\" = \"1\")"));
    }

    @Test
    void mysqlUsesIfNotExistsAndBackticks() {
        String ddl = SinkDdlBuilder.buildCreateTable(config("MYSQL"), "`qa_sink_b37`");

        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS `qa_sink_b37`"));
        assertTrue(ddl.contains("`id` BIGINT"));
        assertFalse(ddl.contains("__dm_sink_seq"));
        assertFalse(ddl.contains("DUPLICATE KEY"));
    }

    @Test
    void oracleHasNoIfNotExistsAndMapsNumericTypes() {
        String ddl = SinkDdlBuilder.buildCreateTable(config("ORACLE"), "\"QA_SINK_B37\"");

        assertTrue(ddl.startsWith("CREATE TABLE \"QA_SINK_B37\" ("));
        assertFalse(ddl.contains("IF NOT EXISTS"));
        assertTrue(ddl.contains("\"id\" NUMBER(19)"));
        assertTrue(ddl.contains("\"name\" VARCHAR2(20)"));
    }

    @Test
    void dorisDdlNeverEmitsUnsigned() {
        SinkConfig config = config("DORIS");
        config.setFieldMapping(Arrays.asList(
                new SinkField("id", "id", "INT UNSIGNED"),
                new SinkField("big", "big", "BIGINT UNSIGNED"),
                new SinkField("small", "small", "TINYINT UNSIGNED"),
                new SinkField("amount", "amount", "DECIMAL(10,2) UNSIGNED")));
        String ddl = SinkDdlBuilder.buildCreateTable(config, "`testdb`.`qa_sink_b37`");

        assertFalse(ddl.contains("UNSIGNED"));
        assertFalse(ddl.contains("ZEROFILL"));
        assertTrue(ddl.contains("`id` BIGINT"));
        assertTrue(ddl.contains("`big` LARGEINT"));
        assertTrue(ddl.contains("`small` SMALLINT"));
        assertTrue(ddl.contains("`amount` DECIMAL(10,2)"));
    }

    @Test
    void dorisUniqueDdlPutsKeysFirstWithoutSequence() {
        SinkConfig config = config("DORIS");
        config.setTableModel(SinkConfig.UNIQUE);
        config.setTableModelExplicit(true);
        config.setKeyColumns(Arrays.asList("name", "id"));
        config.setFieldMapping(Arrays.asList(
                new SinkField("id", "id", "BIGINT"),
                new SinkField("name", "name", "VARCHAR(20)"),
                new SinkField("amount", "amount", "DECIMAL(10,2)")));

        String ddl = SinkDdlBuilder.buildCreateTable(config, "`testdb`.`qa_sink_b37`");

        assertTrue(ddl.startsWith("CREATE TABLE IF NOT EXISTS `testdb`.`qa_sink_b37` ("));
        assertFalse(ddl.contains("__dm_sink_seq"));
        assertFalse(ddl.contains("DUPLICATE KEY"));
        assertTrue(ddl.contains("`name` VARCHAR(20) NOT NULL"));
        assertTrue(ddl.contains("`id` BIGINT NOT NULL"));
        assertTrue(ddl.contains("`amount` DECIMAL(10,2) NULL"));
        assertTrue(ddl.indexOf("`name`") < ddl.indexOf("`id` BIGINT"));
        assertTrue(ddl.indexOf("`id` BIGINT") < ddl.indexOf("`amount`"));
        assertTrue(ddl.contains("UNIQUE KEY(`name`, `id`)"));
        assertTrue(ddl.contains("DISTRIBUTED BY HASH(`name`) BUCKETS 1"));
        assertTrue(ddl.contains("PROPERTIES (\"replication_num\" = \"1\")"));
    }

    @Test
    void dorisDuplicateDdlKeepsSequenceEvenWhenModelExplicit() {
        SinkConfig config = config("DORIS");
        config.setTableModel(SinkConfig.DUPLICATE);
        config.setTableModelExplicit(true);

        String ddl = SinkDdlBuilder.buildCreateTable(config, "`testdb`.`qa_sink_b37`");

        assertTrue(ddl.contains("`__dm_sink_seq` BIGINT NOT NULL"));
        assertTrue(ddl.contains("DUPLICATE KEY(`__dm_sink_seq`)"));
        assertFalse(ddl.contains("UNIQUE KEY"));
    }

    @Test
    void dm8MapsStringToText() {
        SinkConfig config = config("DM8");
        config.setFieldMapping(Arrays.asList(
                new SinkField("id", "id", "BIGINT"),
                new SinkField("name", "name", "STRING")));
        String ddl = SinkDdlBuilder.buildCreateTable(config, "\"QA_SINK_B37\"");

        assertTrue(ddl.contains("\"name\" TEXT"));
        assertTrue(ddl.contains("\"id\" BIGINT"));
    }
}
