package com.datamodel.dto;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 节点数据预览响应；sql + params 唯一可执行，displaySql 仅展示。 */
public class NodeDataPreviewResponse {

    private List<ColumnInfo> columns = new ArrayList<>();
    private List<List<Object>> rows = new ArrayList<>();
    private boolean truncated;
    private String source;
    private String sql;
    private String displaySql;
    private List<Object> params = new ArrayList<>();
    /** Sink 节点附带的写入元信息；不触发任何写入。 */
    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY)
    private Map<String, Object> sink = new LinkedHashMap<>();
    /** 大模型节点预览元信息：是否真实调用了模型等。 */
    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY)
    private Map<String, Object> llm = new LinkedHashMap<>();

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnInfo> columns) {
        this.columns = columns == null ? new ArrayList<>() : columns;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public void setRows(List<List<Object>> rows) {
        this.rows = rows == null ? new ArrayList<>() : rows;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public void setTruncated(boolean truncated) {
        this.truncated = truncated;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getDisplaySql() {
        return displaySql;
    }

    public void setDisplaySql(String displaySql) {
        this.displaySql = displaySql;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params == null ? new ArrayList<>() : params;
    }

    public Map<String, Object> getSink() {
        return sink;
    }

    public void setSink(Map<String, Object> sink) {
        this.sink = sink == null ? new LinkedHashMap<>() : sink;
    }

    public Map<String, Object> getLlm() {
        return llm;
    }

    public void setLlm(Map<String, Object> llm) {
        this.llm = llm == null ? new LinkedHashMap<>() : llm;
    }
}
