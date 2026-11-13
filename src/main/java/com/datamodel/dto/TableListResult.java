package com.datamodel.dto;

import java.util.List;

public class TableListResult {

    private List<TableInfo> list;

    public TableListResult() {
    }

    public TableListResult(List<TableInfo> list) {
        this.list = list;
    }

    public List<TableInfo> getList() {
        return list;
    }

    public void setList(List<TableInfo> list) {
        this.list = list;
    }
}
