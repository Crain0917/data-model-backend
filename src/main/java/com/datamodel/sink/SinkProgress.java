package com.datamodel.sink;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Sink 执行进度快照：每批提交后更新，运行失败时也能回写已提交行数与水位线（M3.7 D-02/D-03）。
 */
public class SinkProgress {

    private String targetType;
    private String targetTable;
    private String writeMode;
    private String sqlText;
    private Map<String, Object> params = new LinkedHashMap<>();

    private long rowsWritten;
    private int batches;
    private int lastBatchNo;

    private String watermarkColumn;
    private String watermarkValue;
    private Long watermarkLastRowid;
    private Long watermarkCommittedRows;
    private boolean tableCreated;

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getSqlText() {
        return sqlText;
    }

    public void setSqlText(String sqlText) {
        this.sqlText = sqlText;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params == null ? new LinkedHashMap<>() : params;
    }

    public long getRowsWritten() {
        return rowsWritten;
    }

    public void setRowsWritten(long rowsWritten) {
        this.rowsWritten = rowsWritten;
    }

    public int getBatches() {
        return batches;
    }

    public void setBatches(int batches) {
        this.batches = batches;
    }

    public int getLastBatchNo() {
        return lastBatchNo;
    }

    public void setLastBatchNo(int lastBatchNo) {
        this.lastBatchNo = lastBatchNo;
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

    public Long getWatermarkLastRowid() {
        return watermarkLastRowid;
    }

    public void setWatermarkLastRowid(Long watermarkLastRowid) {
        this.watermarkLastRowid = watermarkLastRowid;
    }

    public Long getWatermarkCommittedRows() {
        return watermarkCommittedRows;
    }

    public void setWatermarkCommittedRows(Long watermarkCommittedRows) {
        this.watermarkCommittedRows = watermarkCommittedRows;
    }

    public boolean isTableCreated() {
        return tableCreated;
    }

    public void setTableCreated(boolean tableCreated) {
        this.tableCreated = tableCreated;
    }
}
