package com.datamodel.sql;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 与 PRD §12 Filter conditions 同构，Source 过滤复用。
 */
public class Condition {

    private String logic = "AND";
    private int indent;
    private String left;
    private String op;
    private String rightType = "CONST";
    private String right = "";
    private String right2;
    private List<String> rightValues = Collections.emptyList();

    public static List<Condition> parseAll(JsonNode array) {
        List<Condition> result = new ArrayList<>();
        if (array == null || !array.isArray()) {
            return result;
        }
        for (JsonNode item : array) {
            result.add(parse(item));
        }
        return result;
    }

    public static Condition parse(JsonNode item) {
        Condition c = new Condition();
        c.logic = text(item, "logic", "AND");
        c.indent = item.hasNonNull("indent") ? item.get("indent").asInt(0) : 0;
        c.left = text(item, "left", null);
        c.op = text(item, "op", null);
        c.rightType = text(item, "rightType", "CONST");
        c.right2 = text(item, "right2", null);
        JsonNode right = item.get("right");
        if (right == null || right.isNull()) {
            c.right = "";
            c.rightValues = Collections.emptyList();
        } else if (right.isArray()) {
            List<String> values = new ArrayList<>();
            for (JsonNode v : right) {
                values.add(v.isNull() ? "" : v.asText());
            }
            c.rightValues = values;
            c.right = join(values);
        } else {
            c.right = right.asText("");
            c.rightValues = Collections.emptyList();
        }
        return c;
    }

    private static String text(JsonNode item, String field, String fallback) {
        JsonNode v = item.get(field);
        return v == null || v.isNull() ? fallback : v.asText();
    }

    private static String join(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (String v : values) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(v);
        }
        return sb.toString();
    }

    public String getLogic() {
        return logic;
    }

    public int getIndent() {
        return indent;
    }

    public String getLeft() {
        return left;
    }

    public String getOp() {
        return op;
    }

    public String getRightType() {
        return rightType;
    }

    public String getRight() {
        return right;
    }

    public String getRight2() {
        return right2;
    }

    public List<String> getRightValues() {
        return rightValues;
    }

    public boolean isFieldRight() {
        return "FIELD".equalsIgnoreCase(rightType);
    }
}
