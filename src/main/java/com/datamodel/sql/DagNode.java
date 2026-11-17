package com.datamodel.sql;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DagNode {

    private final String id;
    private final String type;
    private final String label;
    private final JsonNode config;
    private final JsonNode raw;

    public DagNode(String id, String type, String label, JsonNode config, JsonNode raw) {
        this.id = id;
        this.type = type;
        this.label = label;
        this.config = config;
        this.raw = raw;
    }

    public static DagNode from(JsonNode node) {
        String id = node.path("id").asText(null);
        String type = normalizeType(node.path("type").asText(""));
        JsonNode data = node.get("data");
        String label = data == null ? null : data.path("label").asText(null);
        JsonNode config = data == null ? null : data.get("config");
        if (config == null || config.isNull()) {
            config = data;
        }
        if (config == null) {
            config = node;
        }
        return new DagNode(id, type, label, config, node);
    }

    public static String normalizeType(String raw) {
        if (raw == null) {
            return "";
        }
        String t = raw.trim();
        if ("groupBy".equalsIgnoreCase(t)) {
            return "groupby";
        }
        return t.toLowerCase();
    }

    public String id() {
        return id;
    }

    public String type() {
        return type;
    }

    public String label() {
        return label;
    }

    public JsonNode config() {
        return config;
    }

    public JsonNode raw() {
        return raw;
    }

    public String text(String field, String fallback) {
        JsonNode v = config.get(field);
        return v == null || v.isNull() ? fallback : v.asText();
    }

    public Long longValue(String field) {
        JsonNode v = config.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            return v.asLong();
        }
        try {
            return Long.valueOf(v.asText());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public boolean boolValue(String field, boolean fallback) {
        JsonNode v = config.get(field);
        if (v == null || v.isNull()) {
            return fallback;
        }
        return v.asBoolean(fallback);
    }

    public JsonNode array(String field) {
        JsonNode v = config.get(field);
        if (v == null || v.isNull()) {
            JsonNode data = raw.get("data");
            v = data == null ? null : data.get(field);
        }
        return v != null && v.isArray() ? v : null;
    }

    /** Source 过滤条件：兼容 config.filter.conditions / config.conditions / 条件数组三种形态。 */
    public List<Condition> sourceFilters() {
        JsonNode filter = config.has("filter") ? config.get("filter") : config.get("conditions");
        if (filter == null || filter.isNull()) {
            return Collections.emptyList();
        }
        if (filter.isArray()) {
            return Condition.parseAll(filter);
        }
        return Condition.parseAll(filter.get("conditions"));
    }

    /** Source 已选字段；空表示全表列。 */
    public List<String> selectedFieldNames() {
        JsonNode fields = array("fields");
        if (fields == null) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<>();
        for (JsonNode f : fields) {
            String name = f.path("name").asText("");
            if (!name.trim().isEmpty()) {
                names.add(name.trim());
            }
        }
        return names;
    }
}
