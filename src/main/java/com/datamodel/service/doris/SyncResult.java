package com.datamodel.service.doris;

import java.util.ArrayList;
import java.util.List;

public class SyncResult {

    private String targetTable;
    private long sourceRows;
    private long loadedRows;
    private String fingerprint;
    private String selectSql;
    private boolean recreated;
    private List<String> warnings = new ArrayList<>();

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(long sourceRows) {
        this.sourceRows = sourceRows;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public void setLoadedRows(long loadedRows) {
        this.loadedRows = loadedRows;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
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
