package com.datamodel.dto;

import java.util.List;

public class ColumnListResult {

    private List<ColumnInfo> list;

    public ColumnListResult() {
    }

    public ColumnListResult(List<ColumnInfo> list) {
        this.list = list;
    }

    public List<ColumnInfo> getList() {
        return list;
    }

    public void setList(List<ColumnInfo> list) {
        this.list = list;
    }
}
