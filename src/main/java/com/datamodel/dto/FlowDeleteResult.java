package com.datamodel.dto;

import java.util.ArrayList;
import java.util.List;

/** 删除任务结果：已删表 + 删除失败表（失败不得静默）。 */
public class FlowDeleteResult {

    private List<String> deletedTables = new ArrayList<>();
    private List<FailedTable> failedTables = new ArrayList<>();

    public List<String> getDeletedTables() {
        return deletedTables;
    }

    public void setDeletedTables(List<String> deletedTables) {
        this.deletedTables = deletedTables == null ? new ArrayList<>() : deletedTables;
    }

    public List<FailedTable> getFailedTables() {
        return failedTables;
    }

    public void setFailedTables(List<FailedTable> failedTables) {
        this.failedTables = failedTables == null ? new ArrayList<>() : failedTables;
    }

    public static class FailedTable {
        private String dorisTable;
        private String reason;

        public FailedTable() {
        }

        public FailedTable(String dorisTable, String reason) {
            this.dorisTable = dorisTable;
            this.reason = reason;
        }

        public String getDorisTable() {
            return dorisTable;
        }

        public void setDorisTable(String dorisTable) {
            this.dorisTable = dorisTable;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}
