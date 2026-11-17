package com.datamodel.sql;

public class OutputColumn {

    private final String name;
    private final String dorisType;

    public OutputColumn(String name, String dorisType) {
        this.name = name;
        this.dorisType = dorisType == null || dorisType.trim().isEmpty() ? "STRING" : dorisType.trim();
    }

    public String getName() {
        return name;
    }

    public String getDorisType() {
        return dorisType;
    }
}
