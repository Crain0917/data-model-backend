package com.datamodel.service.llm;

import java.util.Collections;
import java.util.List;

/**
 * 一行待推理数据：id 为上游 __dm_rowid（字符串回传避免精度丢失）；
 * fields/values 与取数 SQL 的列顺序一一对应。
 */
public record LlmRow(long id, List<String> fields, List<String> values) {

    public LlmRow {
        fields = fields == null ? List.of() : Collections.unmodifiableList(fields);
        values = values == null ? List.of() : Collections.unmodifiableList(values);
    }

    public String idText() {
        return Long.toString(id);
    }

    /** 按字段名取值（大小写不敏感）；不存在返回空串。 */
    public String value(String field) {
        if (field == null) {
            return "";
        }
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).equalsIgnoreCase(field)) {
                return i < values.size() && values.get(i) != null ? values.get(i) : "";
            }
        }
        return "";
    }
}
