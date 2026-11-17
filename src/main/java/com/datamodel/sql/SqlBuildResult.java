package com.datamodel.sql;

import java.util.ArrayList;
import java.util.List;

public class SqlBuildResult {

    private String nodeId;
    private String nodeType;
    private String sql;
    private List<Object> params = new ArrayList<>();
    private String displaySql;
    private List<String> upstreamMissing = new ArrayList<>();
    private List<OutputColumn> outputColumns = new ArrayList<>();

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

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params == null ? new ArrayList<>() : params;
    }

    public String getDisplaySql() {
        return displaySql;
    }

    public void setDisplaySql(String displaySql) {
        this.displaySql = displaySql;
    }

    public List<String> getUpstreamMissing() {
        return upstreamMissing;
    }

    public void setUpstreamMissing(List<String> upstreamMissing) {
        this.upstreamMissing = upstreamMissing == null ? new ArrayList<>() : upstreamMissing;
    }

    public List<OutputColumn> getOutputColumns() {
        return outputColumns;
    }

    public void setOutputColumns(List<OutputColumn> outputColumns) {
        this.outputColumns = outputColumns == null ? new ArrayList<>() : outputColumns;
    }
}
