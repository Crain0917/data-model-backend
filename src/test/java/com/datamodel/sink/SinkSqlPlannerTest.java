package com.datamodel.sink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkSqlPlannerTest {

    private static SinkConfig config(String watermark) {
        SinkConfig config = new SinkConfig();
        config.setNodeId("sink-1");
        config.setTableName("qa_sink");
        config.setWatermarkColumn(watermark);
        config.setFieldMapping(Arrays.asList(
                new SinkField("id", "id", "BIGINT"),
                new SinkField("status", "state", "VARCHAR(20)")));
        return config;
    }

    @Test
    void watermarkResumeUsesCompositePredicateAndStableOrder() {
        SinkSqlPlanner.SinkReadPlan plan = SinkSqlPlanner.plan("dm_mid_1_sink_up", config("updated_at"));

        assertTrue(plan.hasWatermark());
        assertTrue(plan.getSql().contains("SELECT `id`, `status`, `__dm_rowid`"));
        assertTrue(plan.getSql().contains("FROM `dm_mid_1_sink_up`"));
        assertTrue(plan.getSql().contains(
                "WHERE (`updated_at` > ? OR (`updated_at` = ? AND `__dm_rowid` > ?))"));
        assertTrue(plan.getSql().contains("ORDER BY `updated_at`, `__dm_rowid`"));
        assertEquals(Arrays.asList("id", "status", "__dm_rowid"), plan.getReadColumns());
    }

    @Test
    void noWatermarkReadsFullTableWithoutPredicate() {
        SinkSqlPlanner.SinkReadPlan plan = SinkSqlPlanner.plan("dm_src_28_orders", config(null));

        assertFalse(plan.hasWatermark());
        assertFalse(plan.getSql().contains("WHERE"));
        assertFalse(plan.getSql().contains("ORDER BY"));
        assertTrue(plan.getSql().startsWith("SELECT `id`, `status`, `__dm_rowid`"));
    }

    @Test
    void predicateKeepsDuplicateWatermarkValuesDistinctByRowid() {
        assertEquals("`wc` > ? OR (`wc` = ? AND `__dm_rowid` > ?)",
                SinkSqlPlanner.predicate("wc"));
    }

    @Test
    void targetColumnsExcludeInternalColumns() {
        assertEquals(Arrays.asList("id", "state"), SinkSqlPlanner.targetColumns(config("wc")));
        assertTrue(SinkSqlPlanner.isHidden("__dm_rowid"));
        assertTrue(SinkSqlPlanner.isHidden("__dm_sink_seq"));
        assertFalse(SinkSqlPlanner.isHidden("id"));
    }
}
