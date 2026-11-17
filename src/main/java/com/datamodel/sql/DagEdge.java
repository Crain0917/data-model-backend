package com.datamodel.sql;

public class DagEdge {

    private final String id;
    private final String source;
    private final String target;
    private final String sourceHandle;
    private final String targetHandle;

    public DagEdge(String id, String source, String target, String sourceHandle, String targetHandle) {
        this.id = id;
        this.source = source;
        this.target = target;
        this.sourceHandle = sourceHandle;
        this.targetHandle = targetHandle;
    }

    public String getId() {
        return id;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getSourceHandle() {
        return sourceHandle;
    }

    public String getTargetHandle() {
        return targetHandle;
    }
}
