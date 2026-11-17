package com.datamodel.dto;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqlPreviewResponse {

    private String sql;
    private List<Object> params = new ArrayList<>();
    private String displaySql;
    private String dialect = "DORIS";
    private List<String> upstreamMissing = new ArrayList<>();
    /** Sink 节点额外元信息：targetType/targetTable/writeMode/fieldMapping。 */
    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY)
    private Map<String, Object> sink = new LinkedHashMap<>();

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

    public String getDialect() {
        return dialect;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public List<String> getUpstreamMissing() {
        return upstreamMissing;
    }

    public void setUpstreamMissing(List<String> upstreamMissing) {
        this.upstreamMissing = upstreamMissing == null ? new ArrayList<>() : upstreamMissing;
    }

    public Map<String, Object> getSink() {
        return sink;
    }

    public void setSink(Map<String, Object> sink) {
        this.sink = sink == null ? new LinkedHashMap<>() : sink;
    }
}
