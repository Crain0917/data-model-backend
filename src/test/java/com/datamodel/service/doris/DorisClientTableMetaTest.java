package com.datamodel.service.doris;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DorisClientTableMetaTest {

    @Test
    void parsesUniqueModelAndKeyOrder() {
        String ddl = "CREATE TABLE `qa_unique` (\n"
                + "  `id` BIGINT NOT NULL,\n"
                + "  `day` DATE NOT NULL,\n"
                + "  `amount` DECIMAL(10,2) NULL\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`id`, `day`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")";

        DorisClient.TableMeta meta = DorisClient.parseTableMeta(ddl);

        assertEquals("UNIQUE", meta.getModel());
        assertEquals(Arrays.asList("id", "day"), meta.getKeyColumns());
    }

    @Test
    void parsesDuplicateModelHiddenSequenceKey() {
        String ddl = "CREATE TABLE `qa_dup` (\n"
                + "  `__dm_sink_seq` BIGINT NOT NULL,\n"
                + "  `id` BIGINT NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`__dm_sink_seq`)\n"
                + "DISTRIBUTED BY HASH(`__dm_sink_seq`) BUCKETS 1";

        DorisClient.TableMeta meta = DorisClient.parseTableMeta(ddl);

        assertEquals("DUPLICATE", meta.getModel());
        assertEquals(Arrays.asList("__dm_sink_seq"), meta.getKeyColumns());
    }

    @Test
    void unparsableDdlReturnsNoModel() {
        DorisClient.TableMeta meta = DorisClient.parseTableMeta("CREATE TABLE t (`id` INT)");

        assertNull(meta.getModel());
        assertTrue(meta.getKeyColumns().isEmpty());
    }
}
