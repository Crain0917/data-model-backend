package com.datamodel.sql;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;

import java.util.regex.Pattern;

public final class Identifiers {

    public enum QuoteStyle {
        BACKTICK,
        DOUBLE,
        NONE
    }

    private static final Pattern UNSAFE = Pattern.compile("[`\\x00-\\x1f;]");

    private Identifiers() {
    }

    /** 字段/别名允许中文等非 ASCII，但拒绝反引号、控制字符与分号（会破坏引号包裹）。 */
    public static String requireField(String name) {
        if (name == null || name.trim().isEmpty() || UNSAFE.matcher(name).find()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "字段名不合法：" + name);
        }
        return name.trim();
    }

    public static String requireAlias(String name, String fallback) {
        if (name == null || name.trim().isEmpty()) {
            return fallback;
        }
        if (UNSAFE.matcher(name).find()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "别名不合法：" + name);
        }
        return name.trim();
    }

    public static String quote(String name, QuoteStyle style) {
        if (name == null) {
            return "NULL";
        }
        switch (style) {
            case BACKTICK:
                return "`" + name.replace("`", "``") + "`";
            case DOUBLE:
                return "\"" + name.replace("\"", "\"\"") + "\"";
            default:
                return name;
        }
    }

    public static String ref(String alias, String field, QuoteStyle style) {
        String f = quote(requireField(field), style);
        if (alias == null || alias.trim().isEmpty()) {
            return f;
        }
        return quote(requireAlias(alias, "a"), style) + "." + f;
    }

    /** 拒绝注释符 / 多语句注入面，用于用户手写的 ON 片段。 */
    public static String requireRawExpression(String expression) {
        if (expression == null) {
            return null;
        }
        String e = expression.trim();
        if (e.isEmpty()) {
            return null;
        }
        if (e.contains(";") || e.contains("--") || e.contains("/*") || e.contains("*/")) {
            throw new BizException(ErrorCode.PARAM_ERROR, "额外 ON 条件包含非法字符");
        }
        return e;
    }
}
