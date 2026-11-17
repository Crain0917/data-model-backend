package com.datamodel.dto;

import java.util.ArrayList;
import java.util.List;

public class NodeLogsVO {

    private String sqlText;
    private String sqlParams;
    private List<RunLogVO> list = new ArrayList<>();

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

    public List<RunLogVO> getList() {
        return list;
    }

    public void setList(List<RunLogVO> list) {
        this.list = list == null ? new ArrayList<>() : list;
    }
}
