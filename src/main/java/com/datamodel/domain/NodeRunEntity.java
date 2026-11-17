package com.datamodel.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.time.LocalDateTime;

@TableName("data_model_node_run")
public class NodeRunEntity {

    @TableId(type = IdType.AUTO)
    private Long id;
    private Long flowRunId;
    private Long flowId;
    private String nodeId;
    private String nodeType;
    private String nodeName;
    private String status;
    private String sqlText;
    private String params;
    private Long rowCount;
    private Long costMs;
    private String errMsg;
    private String failStage;
    private Integer errorCode;
    private String writeMode;
    private String watermarkColumn;
    private String watermarkValue;
    private Long watermarkRowid;
    private Long committedRows;
    private Integer lastBatchNo;
    private String targetTable;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private LocalDateTime createTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getFlowRunId() {
        return flowRunId;
    }

    public void setFlowRunId(Long flowRunId) {
        this.flowRunId = flowRunId;
    }

    public Long getFlowId() {
        return flowId;
    }

    public void setFlowId(Long flowId) {
        this.flowId = flowId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSqlText() {
        return sqlText;
    }

    public void setSqlText(String sqlText) {
        this.sqlText = sqlText;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public void setRowCount(Long rowCount) {
        this.rowCount = rowCount;
    }

    public Long getCostMs() {
        return costMs;
    }

    public void setCostMs(Long costMs) {
        this.costMs = costMs;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getFailStage() {
        return failStage;
    }

    public void setFailStage(String failStage) {
        this.failStage = failStage;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getWatermarkColumn() {
        return watermarkColumn;
    }

    public void setWatermarkColumn(String watermarkColumn) {
        this.watermarkColumn = watermarkColumn;
    }

    public String getWatermarkValue() {
        return watermarkValue;
    }

    public void setWatermarkValue(String watermarkValue) {
        this.watermarkValue = watermarkValue;
    }

    public Long getWatermarkRowid() {
        return watermarkRowid;
    }

    public void setWatermarkRowid(Long watermarkRowid) {
        this.watermarkRowid = watermarkRowid;
    }

    public Long getCommittedRows() {
        return committedRows;
    }

    public void setCommittedRows(Long committedRows) {
        this.committedRows = committedRows;
    }

    public Integer getLastBatchNo() {
        return lastBatchNo;
    }

    public void setLastBatchNo(Integer lastBatchNo) {
        this.lastBatchNo = lastBatchNo;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }
}
