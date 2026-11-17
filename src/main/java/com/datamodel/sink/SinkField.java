package com.datamodel.sink;

/** 一个上游输出字段到目标列的映射。type 为上游 Doris 侧类型。 */
public class SinkField {

    private final String source;
    private final String target;
    private final String type;

    public SinkField(String source, String target, String type) {
        this.source = source;
        this.target = target;
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getType() {
        return type;
    }
}
