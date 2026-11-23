package com.datamodel.dto;

import com.datamodel.domain.NodeRunEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class NodeRunVOTest {

    @Test
    void exposesSinkProgressOnFailedNode() {
        NodeRunEntity entity = new NodeRunEntity();
        entity.setNodeId("node_2");
        entity.setNodeType("sink");
        entity.setStatus("FAILED");
        entity.setRowCount(300L);
        entity.setWriteMode("APPEND");
        entity.setFailStage("SINK_WRITE");
        entity.setErrorCode(4008);
        entity.setSqlText("INSERT INTO qa_sink(...) 批插 → MYSQL:qa_sink");
        entity.setParams("{\"batchSize\":100}");
        entity.setWatermarkColumn("ts");
        entity.setWatermarkValue("2026-01-01 00:00:00");
        entity.setWatermarkRowid(300L);
        entity.setCommittedRows(300L);
        entity.setLastBatchNo(3);

        NodeRunVO vo = NodeRunVO.from(entity);

        assertEquals(300L, vo.getRowCount());
        assertEquals("APPEND", vo.getWriteMode());
        assertEquals("SINK_WRITE", vo.getFailStage());
        assertEquals(4008, vo.getErrorCode());
        assertEquals("INSERT INTO qa_sink(...) 批插 → MYSQL:qa_sink", vo.getSqlText());
        assertEquals("{\"batchSize\":100}", vo.getParams());
        assertNotNull(vo.getWatermark());
        assertEquals("ts", vo.getWatermark().getColumn());
        assertEquals("2026-01-01 00:00:00", vo.getWatermark().getValue());
        assertEquals(300L, vo.getWatermark().getLastRowid());
        assertEquals(300L, vo.getWatermark().getCommittedRows());
        assertEquals(3, vo.getWatermark().getLastBatchNo());
    }

    @Test
    void successNodeHasNoWatermarkWhenNeverAdvanced() {
        NodeRunEntity entity = new NodeRunEntity();
        entity.setNodeId("node_1");
        entity.setNodeType("source");
        entity.setStatus("SUCCESS");

        NodeRunVO vo = NodeRunVO.from(entity);

        assertNull(vo.getWatermark());
        assertNull(vo.getFailStage());
        assertNull(vo.getErrorCode());
    }
}
