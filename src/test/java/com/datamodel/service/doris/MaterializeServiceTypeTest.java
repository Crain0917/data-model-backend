package com.datamodel.service.doris;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** 直接覆盖 MaterializeService 的 DDL 类型归一化（D-M3-01 回归）。 */
class MaterializeServiceTypeTest {

    private final MaterializeService service = new MaterializeService(
            null, null, null, null, new TypeMappingRegistry());

    @Test
    void unsignedIntegersAreWidened() {
        assertEquals("SMALLINT", service.normalizeType("tinyint unsigned"));
        assertEquals("INT", service.normalizeType("smallint unsigned"));
        assertEquals("INT", service.normalizeType("mediumint unsigned"));
        assertEquals("BIGINT", service.normalizeType("int unsigned"));
        assertEquals("LARGEINT", service.normalizeType("bigint unsigned"));
    }

    @Test
    void sourceTypesUseRegistry() {
        assertEquals("DATETIMEV2(3)", service.normalizeType("datetime(3)"));
        assertEquals("DECIMAL(18,2)", service.normalizeType("decimal(18,2)"));
        assertEquals("VARCHAR(64)", service.normalizeType("varchar(64)"));
        assertEquals("STRING", service.normalizeType("time"));
        assertEquals("STRING", service.normalizeType("enum('a','b')"));
        assertEquals("STRING", service.normalizeType("blob"));
        assertEquals("DATETIME", service.normalizeType("timestamp"));
    }

    @Test
    void emptyTypeFallsBackToString() {
        assertEquals("STRING", service.normalizeType(""));
        assertEquals("STRING", service.normalizeType(null));
    }

    @Test
    void alreadyDorisTypesAreIdempotent() {
        assertIdempotent("LARGEINT");
        assertIdempotent("DATETIMEV2(3)");
        assertIdempotent("DECIMAL(18,2)");
        assertIdempotent("STRING");
        assertIdempotent("BOOLEAN");
        assertIdempotent("BIGINT");
    }

    @Test
    void noUnsignedLeaksIntoDdl() {
        assertFalse(service.normalizeType("bigint unsigned").toUpperCase().contains("UNSIGNED"));
        TypeMapping warn = new TypeMappingRegistry().normalizeDorisType("unknown_type");
        assertEquals("STRING", warn.getDorisType());
        assertNotNull(warn.getWarning());
    }

    private void assertIdempotent(String type) {
        String once = service.normalizeType(type);
        assertEquals(type, once);
        assertEquals(once, service.normalizeType(once));
    }
}
