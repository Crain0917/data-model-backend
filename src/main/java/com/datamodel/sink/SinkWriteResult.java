package com.datamodel.sink;

import java.util.LinkedHashMap;
import java.util.Map;

public class SinkWriteResult {

    private String targetType;
    private String targetTable;
    private String writeMode;
    private long rows;
    private int batches;
    private String sqlText;
    private final Map<String, Object> params = new LinkedHashMap<>();

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

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public int getBatches() {
        return batches;
    }

    public void setBatches(int batches) {
        this.batches = batches;
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
}
