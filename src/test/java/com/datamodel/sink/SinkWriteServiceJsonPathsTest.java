package com.datamodel.sink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SinkWriteServiceJsonPathsTest {

    private static SinkConfig config() {
        SinkConfig config = new SinkConfig();
        config.setFieldMapping(Arrays.asList(
                new SinkField("id", "id", "BIGINT"),
                new SinkField("amount", "amount", "DECIMAL(10,2)")));
        return config;
    }

    @Test
    void sequenceKeyComesFirstToMatchPhysicalColumnOrder() {
        assertEquals(Arrays.asList("$.__dm_sink_seq", "$.id", "$.amount"),
                SinkWriteService.dorisJsonPaths(config(),
                        Arrays.asList("__dm_sink_seq", "id", "amount")));
    }

    @Test
    void existingTableWithoutSequenceKeyKeepsPhysicalOrder() {
        assertEquals(Arrays.asList("$.id", "$.amount"),
                SinkWriteService.dorisJsonPaths(config(), Arrays.asList("id", "amount")));
    }

    @Test
    void followsPhysicalOrderNotMappingOrder() {
        assertEquals(Arrays.asList("$.amount", "$.id"),
                SinkWriteService.dorisJsonPaths(config(), Arrays.asList("amount", "id")));
    }

    @Test
    void sequenceKeyInMiddleFollowsExistingTableOrder() {
        assertEquals(Arrays.asList("$.id", "$.__dm_sink_seq", "$.amount"),
                SinkWriteService.dorisJsonPaths(config(),
                        Arrays.asList("id", "__dm_sink_seq", "amount")));
    }

    @Test
    void uniqueTableKeysReorderJsonPaths() {
        SinkConfig config = new SinkConfig();
        config.setTableModel(SinkConfig.UNIQUE);
        config.setKeyColumns(Arrays.asList("id"));
        config.setFieldMapping(Arrays.asList(
                new SinkField("src_amount", "amount", "DECIMAL(10,2)"),
                new SinkField("src_id", "id", "BIGINT")));

        assertEquals(Arrays.asList("$.id", "$.amount"),
                SinkWriteService.dorisJsonPaths(config, Arrays.asList("id", "amount")));
    }

    @Test
    void jsonBodyKeysUsePhysicalColumnCase() {
        SinkConfig config = new SinkConfig();
        config.setFieldMapping(Arrays.asList(
                new SinkField("id", "ID", "BIGINT"),
                new SinkField("amount", "Amount", "DECIMAL(10,2)")));

        assertArrayEquals(new String[]{"id", "amount"},
                SinkWriteService.jsonBodyKeys(config, Arrays.asList("id", "amount")));
    }
}
