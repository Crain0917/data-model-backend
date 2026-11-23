package com.datamodel.service.doris;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypeMappingRegistryTest {

    private final TypeMappingRegistry registry = new TypeMappingRegistry();

    // ---------------------------------------------------------------- MySQL

    @Test
    void mysqlIntegers() {
        assertType("MYSQL", "TINYINT", "TINYINT");
        assertType("MYSQL", "TINYINT UNSIGNED", "SMALLINT");
        assertType("MYSQL", "SMALLINT", "SMALLINT");
        assertType("MYSQL", "SMALLINT UNSIGNED", "INT");
        assertType("MYSQL", "MEDIUMINT", "INT");
        assertType("MYSQL", "INT", "INT");
        assertType("MYSQL", "INT UNSIGNED", "BIGINT");
        assertType("MYSQL", "BIGINT", "BIGINT");
        assertType("MYSQL", "BIGINT UNSIGNED", "LARGEINT");
    }

    @Test
    void mysqlDecimal() {
        assertType("MYSQL", "DECIMAL(10,2)", "DECIMAL(10,2)");
        assertType("MYSQL", "DECIMAL(38,0)", "DECIMAL(38,0)");
        assertFallback("MYSQL", "DECIMAL(40,2)", "STRING");
        assertFallback("MYSQL", "DECIMAL", "STRING");
    }

    @Test
    void mysqlStringTypes() {
        assertType("MYSQL", "CHAR(10)", "CHAR(10)");
        assertType("MYSQL", "CHAR(300)", "VARCHAR(300)");
        assertFallback("MYSQL", "CHAR(70000)", "STRING");
        assertType("MYSQL", "VARCHAR(255)", "VARCHAR(255)");
        assertType("MYSQL", "VARCHAR(65533)", "VARCHAR(65533)");
        assertFallback("MYSQL", "VARCHAR(65534)", "STRING");
        assertType("MYSQL", "TEXT", "STRING");
        assertType("MYSQL", "LONGTEXT", "STRING");
        assertType("MYSQL", "BLOB", "STRING");
        assertType("MYSQL", "LONGBLOB", "STRING");
    }

    @Test
    void mysqlDateAndTime() {
        assertType("MYSQL", "DATE", "DATE");
        assertType("MYSQL", "DATETIME", "DATETIME");
        assertType("MYSQL", "DATETIME(3)", "DATETIMEV2(3)");
        assertType("MYSQL", "DATETIME(6)", "DATETIMEV2(6)");
        TypeMapping timestamp = registry.map("MYSQL", "TIMESTAMP");
        assertEquals("DATETIME", timestamp.getDorisType());
        assertNotNull(timestamp.getWarning());
        TypeMapping time = registry.map("MYSQL", "TIME");
        assertEquals("STRING", time.getDorisType());
        assertNotNull(time.getWarning());
        assertType("MYSQL", "YEAR", "SMALLINT");
    }

    @Test
    void mysqlEnumSetBit() {
        TypeMapping e = registry.map("MYSQL", "ENUM('a','b')");
        assertEquals("STRING", e.getDorisType());
        assertNotNull(e.getWarning());
        TypeMapping s = registry.map("MYSQL", "SET('x','y')");
        assertEquals("STRING", s.getDorisType());
        assertNotNull(s.getWarning());
        assertType("MYSQL", "BIT(1)", "BOOLEAN");
        assertType("MYSQL", "BIT", "BOOLEAN");
        assertType("MYSQL", "BIT(8)", "BIGINT");
        assertType("MYSQL", "BIT(64)", "BIGINT");
        assertFallback("MYSQL", "BIT(65)", "STRING");
    }

    @Test
    void mysqlUnknownFallsBack() {
        TypeMapping unknown = registry.map("MYSQL", "GEOMETRYCOLLECTION");
        assertEquals("STRING", unknown.getDorisType());
        assertTrue(unknown.isFallback());
        assertNotNull(unknown.getWarning());
    }

    // --------------------------------------------------------------- Oracle

    @Test
    void oracleNumber() {
        assertType("ORACLE", "NUMBER(10,2)", "DECIMAL(10,2)");
        TypeMapping noPrecision = registry.map("ORACLE", "NUMBER");
        assertEquals("DOUBLE", noPrecision.getDorisType());
        assertNotNull(noPrecision.getWarning());
        assertFallback("ORACLE", "NUMBER(40,2)", "STRING");
    }

    @Test
    void oracleTypes() {
        assertType("ORACLE", "INTEGER", "DECIMAL(38,0)");
        assertType("ORACLE", "VARCHAR2(100)", "VARCHAR(100)");
        assertType("ORACLE", "CLOB", "STRING");
        assertType("ORACLE", "NCLOB", "STRING");
        assertType("ORACLE", "BLOB", "STRING");
        assertType("ORACLE", "RAW(16)", "STRING");
        assertType("ORACLE", "DATE", "DATETIME");
        assertType("ORACLE", "TIMESTAMP(6)", "DATETIMEV2(6)");
        TypeMapping tz = registry.map("ORACLE", "TIMESTAMP WITH TIME ZONE");
        assertEquals("DATETIMEV2(0)", tz.getDorisType());
        assertNotNull(tz.getWarning());
        assertType("ORACLE", "BOOLEAN", "BOOLEAN");
        assertFallback("ORACLE", "SDO_GEOMETRY", "STRING");
    }

    // ----------------------------------------------------------------- DM8

    @Test
    void dmTypes() {
        TypeMapping f = registry.map("DM8", "FLOAT");
        assertEquals("DOUBLE", f.getDorisType());
        assertNotNull(f.getWarning());
        assertType("DM8", "DATE", "DATE");
        TypeMapping time = registry.map("DM8", "TIME");
        assertEquals("STRING", time.getDorisType());
        assertNotNull(time.getWarning());
        assertType("DM8", "DATETIME", "DATETIME");
        assertType("DM8", "TIMESTAMP", "DATETIME");
        assertType("DM8", "BIT", "BOOLEAN");
        assertType("DM8", "BOOLEAN", "BOOLEAN");
        assertType("DM8", "VARBINARY(100)", "STRING");
        assertType("DM8", "NUMBER(12,4)", "DECIMAL(12,4)");
        assertFallback("DM8", "UNKNOWN_TYPE", "STRING");
    }

    @Test
    void emptyTypeFallsBack() {
        TypeMapping empty = registry.map("MYSQL", " ");
        assertEquals("STRING", empty.getDorisType());
        assertTrue(empty.isFallback());
    }

    private void assertType(String dialect, String raw, String expected) {
        TypeMapping mapping = registry.map(dialect, raw);
        assertEquals(expected, mapping.getDorisType(), dialect + " " + raw);
        assertFalse(mapping.isFallback(), dialect + " " + raw + " 不应兜底");
    }

    private void assertFallback(String dialect, String raw, String expected) {
        TypeMapping mapping = registry.map(dialect, raw);
        assertEquals(expected, mapping.getDorisType(), dialect + " " + raw);
        assertTrue(mapping.isFallback(), dialect + " " + raw + " 应标记兜底");
        assertNotNull(mapping.getWarning());
    }
}
