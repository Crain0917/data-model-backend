package com.datamodel.service.doris;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DorisNameSanitizerTest {

    private final DorisNameSanitizer sanitizer = new DorisNameSanitizer();

    @Test
    void sourceAndMiddleTableNames() {
        assertEquals("dm_src_12_orders", sanitizer.sourceTable(12, "Orders"));
        assertEquals("dm_src_12_order_detail", sanitizer.sourceTable(12, "order-detail"));
        assertEquals("dm_mid_101_node_61", sanitizer.middleTable(101, "node_61"));
        assertEquals("dm_mid_101_node_61", sanitizer.middleTable(101, "Node-61"));
    }

    @Test
    void sanitizePartCollapsesAndTrims() {
        assertEquals("a_b_c", sanitizer.sanitizePart("a--b__c"));
        assertEquals("orders_2024", sanitizer.sanitizePart("__orders  2024__"));
        assertEquals("unknown", sanitizer.sanitizePart(null));
        assertEquals("unknown", sanitizer.sanitizePart("!!!"));
        assertEquals("1", sanitizer.sanitizePart("订单 表-1"));
    }

    @Test
    void longNameIsTruncatedWithStableHash() {
        StringBuilder raw = new StringBuilder();
        for (int i = 0; i < 80; i++) {
            raw.append('a');
        }
        String first = sanitizer.sourceTable(1, raw.toString());
        String second = sanitizer.sourceTable(1, raw.toString());
        assertEquals(first, second);
        assertEquals(64, first.length());
        assertTrue(first.matches("dm_src_1_a+_[0-9a-f]{6}"), first);
    }

    @Test
    void flowSourceTableKeepsRandomSuffixWithinSixtyFourChars() {
        assertEquals("dm_src_101_orders_ab12cd",
                sanitizer.flowSourceTable(101, "Orders", "ab12cd"));

        StringBuilder raw = new StringBuilder();
        for (int i = 0; i < 120; i++) {
            raw.append('a');
        }
        String table = sanitizer.flowSourceTable(101, raw.toString(), "z9y8x7");
        assertEquals(64, table.length(), table);
        assertTrue(table.startsWith("dm_src_101_a"), table);
        assertTrue(table.endsWith("_z9y8x7"), table);
        assertTrue(table.matches("dm_src_101_a+_[0-9a-f]{6}_z9y8x7"), table);
    }

    @Test
    void columnNameKeepsSafeIdentifiers() {
        assertEquals("order_id", sanitizer.columnName("order_id", 0));
        assertEquals("col_1", sanitizer.columnName("order id", 1));
        assertEquals("col_2", sanitizer.columnName("", 2));
        assertEquals("col_3", sanitizer.columnName("1abc", 3));
    }
}
