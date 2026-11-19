package com.datamodel.dto;

/**
 * 节点错误详情。字段集合与顺序为对外契约，新增字段需同步前端。
 */
public class NodeErrorDetailVO {

    private Long runId;
    private String nodeId;
    private String nodeName;
    private String status;
    private Integer errorCode;
    private String failStage;
    private String errMsg;
    private String sqlText;
    private String sqlParams;
    private String errorUrl;
    private String streamLoadLog;
    private boolean streamLoadLogTruncated;
    private String fetchError;

    public Long getRunId() {
        return runId;
    }

    public void setRunId(Long runId) {
        this.runId = runId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
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

    public Integer getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    public String getFailStage() {
        return failStage;
    }

    public void setFailStage(String failStage) {
        this.failStage = failStage;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getSqlText() {
        return sqlText;
    }

    public void setSqlText(String sqlText) {
        this.sqlText = sqlText;
    }

    public String getSqlParams() {
        return sqlParams;
    }

    public void setSqlParams(String sqlParams) {
        this.sqlParams = sqlParams;
    }

    public String getErrorUrl() {
        return errorUrl;
    }

    public void setErrorUrl(String errorUrl) {
        this.errorUrl = errorUrl;
    }

    public String getStreamLoadLog() {
        return streamLoadLog;
    }

    public void setStreamLoadLog(String streamLoadLog) {
        this.streamLoadLog = streamLoadLog;
    }

    public boolean isStreamLoadLogTruncated() {
        return streamLoadLogTruncated;
    }

    public void setStreamLoadLogTruncated(boolean streamLoadLogTruncated) {
        this.streamLoadLogTruncated = streamLoadLogTruncated;
    }

    public String getFetchError() {
        return fetchError;
    }

    public void setFetchError(String fetchError) {
        this.fetchError = fetchError;
    }
}
