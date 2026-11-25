package com.datamodel.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

public class ColumnInfo {

    private String name;
    private String type;
    private String sourceType;

    public ColumnInfo() {
    }

    public ColumnInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public ColumnInfo(String name, String type, String sourceType) {
        this.name = name;
        this.type = type;
        this.sourceType = sourceType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }
}
