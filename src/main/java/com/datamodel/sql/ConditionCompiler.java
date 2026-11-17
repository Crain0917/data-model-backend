package com.datamodel.sql;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 把 Filter / Source 的 conditions 编译成带括号的 WHERE/HAVING 表达式。
 * 参数化模式下常量一律走 ?，值进 params；展示模式生成已绑值文本。
 */
public final class ConditionCompiler {

    public static final Set<String> SOURCE_OPERATORS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "EQ", "NE", "LT", "LTE", "GT", "GTE", "LIKE", "NOT_LIKE",
            "IS_NULL", "IS_NOT_NULL", "IN", "NOT_IN", "BETWEEN", "NOT_BETWEEN",
            "STARTS_WITH", "ENDS_WITH", "CONTAINS")));

    private static final Set<String> FILTER_OPERATORS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "EQ", "NE", "LT", "LTE", "GT", "GTE", "LIKE", "NOT_LIKE",
            "IS_NULL", "IS_NOT_NULL", "IN", "NOT_IN", "BETWEEN", "NOT_BETWEEN",
            "REGEXP", "STARTS_WITH", "ENDS_WITH", "CONTAINS")));

    private ConditionCompiler() {
    }

    public static Compiled compile(List<Condition> rows, String alias,
                                   Identifiers.QuoteStyle style, boolean parameterized) {
        return compile(rows, alias, style, parameterized, FILTER_OPERATORS);
    }

    public static Compiled compileSource(List<Condition> rows, String alias,
                                         Identifiers.QuoteStyle style, boolean parameterized) {
        return compile(rows, alias, style, parameterized, SOURCE_OPERATORS);
    }

    private static Compiled compile(List<Condition> rows, String alias,
                                    Identifiers.QuoteStyle style, boolean parameterized,
                                    Set<String> allowed) {
        if (rows == null || rows.isEmpty()) {
            return new Compiled("", new ArrayList<>());
        }
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        int idx = 0;
        for (Condition row : rows) {
            if (row.getLeft() == null || row.getLeft().trim().isEmpty()) {
                continue;
            }
            String op = row.getOp() == null ? "" : row.getOp().trim().toUpperCase();
            if (!allowed.contains(op)) {
                throw new BizException(ErrorCode.PARAM_ERROR, "过滤算子不支持：" + row.getOp());
            }
            String expr = expression(row, op, alias, style, parameterized, params);
            if (idx > 0) {
                sql.append('\n');
                int indent = Math.max(0, row.getIndent());
                for (int i = 0; i < indent; i++) {
                    sql.append("  ");
                }
                sql.append(row.getLogic() == null ? "AND" : row.getLogic().trim().toUpperCase()).append(' ');
            }
            sql.append('(').append(expr).append(')');
            idx++;
        }
        return new Compiled(sql.toString(), params);
    }

    private static String expression(Condition row, String op, String alias,
                                     Identifiers.QuoteStyle style, boolean parameterized,
                                     List<Object> params) {
        String left = Identifiers.ref(alias, row.getLeft(), style);
        String keyword = keyword(op);
        if ("IS_NULL".equals(op) || "IS_NOT_NULL".equals(op)) {
            return left + " " + keyword;
        }
        if (row.isFieldRight()) {
            if (row.getRight() == null || row.getRight().trim().isEmpty()) {
                throw new BizException(ErrorCode.PARAM_ERROR, "字段比较缺少右侧字段");
            }
            return left + " " + keyword + " " + Identifiers.ref(alias, row.getRight(), style);
        }
        if ("IN".equals(op) || "NOT_IN".equals(op)) {
            List<String> values = inValues(row);
            if (values.isEmpty()) {
                return left + " " + keyword + " (NULL)";
            }
            StringBuilder sb = new StringBuilder(left).append(' ').append(keyword).append(" (");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(value(values.get(i), parameterized, params));
            }
            return sb.append(')').toString();
        }
        if ("BETWEEN".equals(op) || "NOT_BETWEEN".equals(op)) {
            return left + " " + keyword + " " + value(row.getRight(), parameterized, params)
                    + " AND " + value(row.getRight2(), parameterized, params);
        }
        if ("STARTS_WITH".equals(op)) {
            return left + " LIKE " + value(append(row.getRight(), "%"), parameterized, params);
        }
        if ("ENDS_WITH".equals(op)) {
            return left + " LIKE " + value(prepend(row.getRight(), "%"), parameterized, params);
        }
        if ("CONTAINS".equals(op)) {
            return left + " LIKE " + value("%" + nullToEmpty(row.getRight()) + "%", parameterized, params);
        }
        return left + " " + keyword + " " + value(row.getRight(), parameterized, params);
    }

    private static String keyword(String op) {
        switch (op) {
            case "EQ":
                return "=";
            case "NE":
                return "<>";
            case "LT":
                return "<";
            case "LTE":
                return "<=";
            case "GT":
                return ">";
            case "GTE":
                return ">=";
            case "LIKE":
                return "LIKE";
            case "NOT_LIKE":
                return "NOT LIKE";
            case "REGEXP":
                return "REGEXP";
            case "STARTS_WITH":
            case "ENDS_WITH":
            case "CONTAINS":
                return "LIKE";
            case "IN":
                return "IN";
            case "NOT_IN":
                return "NOT IN";
            case "BETWEEN":
                return "BETWEEN";
            case "NOT_BETWEEN":
                return "NOT BETWEEN";
            case "IS_NULL":
                return "IS NULL";
            case "IS_NOT_NULL":
                return "IS NOT NULL";
            default:
                throw new BizException(ErrorCode.PARAM_ERROR, "过滤算子不支持：" + op);
        }
    }

    private static String value(String raw, boolean parameterized, List<Object> params) {
        if (parameterized) {
            params.add(typed(raw));
            return "?";
        }
        return literal(raw);
    }

    private static Object typed(String raw) {
        String v = raw == null ? "" : raw.trim();
        if (v.matches("-?(0|[1-9]\\d{0,17})")) {
            try {
                return Long.valueOf(v);
            } catch (NumberFormatException ignored) {
                return v;
            }
        }
        if (v.matches("-?(0|[1-9]\\d*)\\.\\d+([eE][+-]?\\d+)?")) {
            try {
                return new BigDecimal(v);
            } catch (NumberFormatException ignored) {
                return v;
            }
        }
        return raw == null ? "" : raw;
    }

    private static String literal(String raw) {
        String v = raw == null ? "" : raw;
        if (v.matches("-?(0|[1-9]\\d*)(\\.\\d+)?([eE][+-]?\\d+)?")) {
            return v;
        }
        return "'" + v.replace("'", "''") + "'";
    }

    private static List<String> inValues(Condition row) {
        if (!row.getRightValues().isEmpty()) {
            return row.getRightValues();
        }
        String raw = row.getRight();
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;
        char[] chars = raw.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (c == '\'' && !inDouble) {
                if (inSingle && i + 1 < chars.length && chars[i + 1] == '\'') {
                    current.append('\'');
                    i++;
                } else {
                    inSingle = !inSingle;
                }
                continue;
            }
            if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                continue;
            }
            if (c == ',' && !inSingle && !inDouble) {
                values.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        values.add(current.toString().trim());
        return values;
    }

    private static String append(String raw, String suffix) {
        return nullToEmpty(raw) + suffix;
    }

    private static String prepend(String raw, String prefix) {
        return prefix + nullToEmpty(raw);
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    public static final class Compiled {
        private final String expr;
        private final List<Object> params;

        public Compiled(String expr, List<Object> params) {
            this.expr = expr;
            this.params = params;
        }

        public String getExpr() {
            return expr;
        }

        public List<Object> getParams() {
            return params;
        }
    }
}
