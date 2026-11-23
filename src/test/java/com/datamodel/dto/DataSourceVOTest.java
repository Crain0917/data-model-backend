package com.datamodel.dto;

import com.datamodel.domain.DataSourceEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSourceVOTest {

    private static DataSourceEntity doris(String extraParams) {
        DataSourceEntity entity = new DataSourceEntity();
        entity.setId(1L);
        entity.setName("doris_demo");
        entity.setType("DORIS");
        entity.setExtraParams(extraParams);
        return entity;
    }

    @Test
    void exposesConfiguredHttpPortForDoris() {
        DataSourceVO vo = DataSourceVO.from(doris("{\"httpPort\":8040}"));
        assertEquals(8040, vo.getHttpPort());
        assertTrue(vo.isHttpPortConfigured());
    }

    @Test
    void marksMissingOrInvalidHttpPortAsNotConfigured() {
        DataSourceVO missing = DataSourceVO.from(doris(null));
        assertNull(missing.getHttpPort());
        assertFalse(missing.isHttpPortConfigured());

        DataSourceVO invalid = DataSourceVO.from(doris("{\"httpPort\":\"abc\"}"));
        assertNull(invalid.getHttpPort());
        assertFalse(invalid.isHttpPortConfigured());
    }

    @Test
    void mysqlNeverReportsHttpPort() {
        DataSourceEntity entity = doris("{\"httpPort\":8040}");
        entity.setType("MYSQL");
        DataSourceVO vo = DataSourceVO.from(entity);
        assertNull(vo.getHttpPort());
        assertFalse(vo.isHttpPortConfigured());
    }
}
