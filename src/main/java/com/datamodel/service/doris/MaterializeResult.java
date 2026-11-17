package com.datamodel.service.doris;

import java.util.ArrayList;
import java.util.List;

public class MaterializeResult {

    private String targetTable;
    private long rows;
    private String sql;
    private boolean recreated;
    private List<String> warnings = new ArrayList<>();

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean isRecreated() {
        return recreated;
    }

    public void setRecreated(boolean recreated) {
        this.recreated = recreated;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public void setWarnings(List<String> warnings) {
        this.warnings = warnings;
    }
}
