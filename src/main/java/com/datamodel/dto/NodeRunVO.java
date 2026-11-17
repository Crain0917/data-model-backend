package com.datamodel.dto;

import com.datamodel.common.FailStage;
import com.datamodel.domain.NodeRunEntity;

import java.time.LocalDateTime;

public class NodeRunVO {

    private String nodeId;
    private String nodeType;
    private String nodeName;
    private String status;
    private Long rowCount;
    private Long costMs;
    private String targetTable;
    private String writeMode;
    private String errMsg;
    private String failStage;
    private Integer errorCode;
    private String sqlText;
    private String params;
    private WatermarkVO watermark;
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    public static NodeRunVO from(NodeRunEntity e) {
        NodeRunVO vo = new NodeRunVO();
        vo.nodeId = e.getNodeId();
        vo.nodeType = e.getNodeType();
        vo.nodeName = e.getNodeName();
        vo.status = e.getStatus();
        vo.rowCount = e.getRowCount();
        vo.costMs = e.getCostMs();
        vo.targetTable = e.getTargetTable();
        vo.writeMode = e.getWriteMode();
        vo.errMsg = e.getErrMsg();
        vo.errorCode = e.getErrorCode();
        vo.failStage = e.getFailStage() != null ? e.getFailStage()
                : ("FAILED".equals(e.getStatus()) ? FailStage.classify(e.getErrMsg()) : null);
        vo.sqlText = e.getSqlText();
        vo.params = e.getParams();
        if (e.getWatermarkColumn() != null || e.getWatermarkValue() != null
                || e.getWatermarkRowid() != null || e.getCommittedRows() != null) {
            vo.watermark = new WatermarkVO(e.getWatermarkColumn(), e.getWatermarkValue(),
                    e.getWatermarkRowid(), e.getCommittedRows(), e.getLastBatchNo());
        }
        vo.startTime = e.getStartTime();
        vo.endTime = e.getEndTime();
        return vo;
    }

    public static class WatermarkVO {

        private final String column;
        private final String value;
        private final Long lastRowid;
        private final Long committedRows;
        private final Integer lastBatchNo;

        public WatermarkVO(String column, String value, Long lastRowid, Long committedRows,
                           Integer lastBatchNo) {
            this.column = column;
            this.value = value;
            this.lastRowid = lastRowid;
            this.committedRows = committedRows;
            this.lastBatchNo = lastBatchNo;
        }

        public String getColumn() {
            return column;
        }

        public String getValue() {
            return value;
        }

        public Long getLastRowid() {
            return lastRowid;
        }

        public Long getCommittedRows() {
            return committedRows;
        }

        public Integer getLastBatchNo() {
            return lastBatchNo;
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeType() {
        return nodeType;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getStatus() {
        return status;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public Long getCostMs() {
        return costMs;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public String getFailStage() {
        return failStage;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public String getSqlText() {
        return sqlText;
    }

    public String getParams() {
        return params;
    }

    public WatermarkVO getWatermark() {
        return watermark;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }
}
