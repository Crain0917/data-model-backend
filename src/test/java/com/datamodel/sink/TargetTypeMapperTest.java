package com.datamodel.sink;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TargetTypeMapperTest {

    @Test
    void mysqlDecimalV3IsCanonicalDecimal() {
        assertEquals("DECIMAL(10, 2)", TargetTypeMapper.toTarget("MYSQL", "decimalv3(10, 2)"));
        assertTrue(TargetTypeMapper.compatible("MYSQL", "decimalv3(10, 2)", "decimal(10,2)"));
    }

    @Test
    void dorisDecimalV3TypeMatchesItsPhysicalAlias() {
        assertTrue(TargetTypeMapper.compatible("DORIS", "decimalv3(10, 2)", "decimal(10,2)"));
    }

    @Test
    void datetimeV3IsCanonicalDatetime() {
        assertTrue(TargetTypeMapper.compatible("MYSQL", "datetimev2(3)", "datetime"));
        assertTrue(TargetTypeMapper.compatible("DORIS", "datetimev2(3)", "datetime"));
    }

    @Test
    void dorisWidensUnsignedIntegers() {
        assertEquals("SMALLINT", TargetTypeMapper.toTarget("DORIS", "TINYINT UNSIGNED"));
        assertEquals("INT", TargetTypeMapper.toTarget("DORIS", "SMALLINT UNSIGNED"));
        assertEquals("INT", TargetTypeMapper.toTarget("DORIS", "MEDIUMINT UNSIGNED"));
        assertEquals("BIGINT", TargetTypeMapper.toTarget("DORIS", "INT UNSIGNED"));
        assertEquals("BIGINT", TargetTypeMapper.toTarget("DORIS", "INTEGER UNSIGNED"));
        assertEquals("BIGINT", TargetTypeMapper.toTarget("DORIS", "INT(10) UNSIGNED"));
        assertEquals("LARGEINT", TargetTypeMapper.toTarget("DORIS", "BIGINT UNSIGNED"));
        assertEquals("BIGINT", TargetTypeMapper.toTarget("DORIS", "int unsigned"));
        assertEquals("BIGINT", TargetTypeMapper.toTarget("DORIS", "INT(10) UNSIGNED ZEROFILL"));
    }

    @Test
    void dorisStripsUnsignedFromNonIntegerTypes() {
        assertEquals("DECIMAL(10,2)", TargetTypeMapper.toTarget("DORIS", "DECIMAL(10,2) UNSIGNED"));
        assertEquals("DOUBLE", TargetTypeMapper.toTarget("DORIS", "DOUBLE UNSIGNED"));
        assertEquals("FLOAT", TargetTypeMapper.toTarget("DORIS", "FLOAT ZEROFILL"));
    }

    @Test
    void noTargetEverEmitsUnsignedOrZerofill() {
        String[] inputs = {
                "TINYINT UNSIGNED", "SMALLINT UNSIGNED", "MEDIUMINT UNSIGNED",
                "INT UNSIGNED", "INT(10) UNSIGNED", "INTEGER UNSIGNED", "BIGINT UNSIGNED",
                "DECIMAL(10,2) UNSIGNED", "DOUBLE UNSIGNED", "FLOAT UNSIGNED ZEROFILL"
        };
        for (String target : new String[]{"DORIS", "MYSQL", "DM8", "ORACLE"}) {
            for (String input : inputs) {
                String out = TargetTypeMapper.toTarget(target, input).toUpperCase(Locale.ROOT);
                assertFalse(out.contains("UNSIGNED"), target + " " + input + " -> " + out);
                assertFalse(out.contains("ZEROFILL"), target + " " + input + " -> " + out);
            }
        }
    }

    @Test
    void unsignedDoesNotFallBackToTextOnOtherTargets() {
        assertEquals("BIGINT", TargetTypeMapper.toTarget("MYSQL", "INT UNSIGNED"));
        assertEquals("BIGINT", TargetTypeMapper.toTarget("DM8", "INT UNSIGNED"));
        assertEquals("NUMBER(19)", TargetTypeMapper.toTarget("ORACLE", "INT UNSIGNED"));
    }

    @Test
    void widenedTypeIsCompatibleWithPhysicalDorisType() {
        assertTrue(TargetTypeMapper.compatible("DORIS", "INT UNSIGNED", "BIGINT"));
        assertTrue(TargetTypeMapper.compatible("DORIS", "BIGINT UNSIGNED", "LARGEINT"));
    }
}
