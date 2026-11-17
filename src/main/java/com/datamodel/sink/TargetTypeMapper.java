package com.datamodel.sink;

import java.util.Locale;

/**
 * Doris 逻辑类型 → 目标库物理类型，以及目标库类型归一化（架构 M3.7 §20/§21）。
 */
public final class TargetTypeMapper {

    private TargetTypeMapper() {
    }

    /** 按目标类型生成建表用类型。 */
    public static String toTarget(String targetType, String dorisType) {
        String type = normalizeDoris(dorisType);
        String base = baseDoris(type);
        String target = targetType == null ? SinkConfig.DORIS : targetType.toUpperCase(Locale.ROOT);
        switch (target) {
            case "MYSQL":
                return mysql(base, type);
            case "ORACLE":
                return oracle(base, type);
            case "DM8":
                return dm8(base, type);
            default:
                return doris(type);
        }
    }

    /** 归一化目标库实际类型，用于结构一致性比较。 */
    public static String canonical(String targetType, String actualType) {
        if (actualType == null) {
            return "";
        }
        String t = actualType.trim().toUpperCase(Locale.ROOT);
        int paren = t.indexOf('(');
        String base = paren > 0 ? t.substring(0, paren) : t;
        base = base.replace("UNSIGNED", "").replace("ZEROFILL", "").trim();
        String target = targetType == null ? SinkConfig.DORIS : targetType.toUpperCase(Locale.ROOT);
        switch (target) {
            case "MYSQL":
                if ("INTEGER".equals(base)) {
                    base = "INT";
                } else if ("BOOL".equals(base) || "BIT".equals(base)) {
                    base = "TINYINT";
                } else if ("NUMERIC".equals(base)) {
                    base = "DECIMAL";
                } else if ("DOUBLE PRECISION".equals(base)) {
                    base = "DOUBLE";
                }
                break;
            case "ORACLE":
                if (base.startsWith("TIMESTAMP")) {
                    base = "TIMESTAMP";
                } else if ("NUMERIC".equals(base) || "DEC".equals(base)) {
                    base = "NUMBER";
                } else if ("INTEGER".equals(base)) {
                    base = "NUMBER";
                } else if ("BINARY_FLOAT".equals(base) || "BINARY_DOUBLE".equals(base)) {
                    base = "DOUBLE";
                }
                break;
            case "DM8":
                if (base.startsWith("TIMESTAMP")) {
                    base = "TIMESTAMP";
                } else if ("NUMERIC".equals(base) || "NUMBER".equals(base) || "DEC".equals(base)) {
                    base = "DECIMAL";
                } else if ("INTEGER".equals(base)) {
                    base = "INT";
                } else if ("BOOLEAN".equals(base)) {
                    base = "BIT";
                } else if ("TEXT".equals(base) || "CLOB".equals(base)) {
                    base = "VARCHAR";
                }
                break;
            default:
                if ("INTEGER".equals(base)) {
                    base = "INT";
                } else if ("BOOL".equals(base)) {
                    base = "BOOLEAN";
                } else if (base.startsWith("DATETIME")) {
                    base = "DATETIME";
                } else if ("TEXT".equals(base) || "VARCHAR".equals(base) || "CHAR".equals(base)) {
                    base = "STRING";
                }
                break;
        }
        return family(base);
    }

    /** 兼容性比较：只比类型家族，忽略长度/精度差异。 */
    public static boolean compatible(String targetType, String expectedDorisType, String actualType) {
        String expected = toTarget(targetType, expectedDorisType);
        return canonical(targetType, expected).equals(canonical(targetType, actualType));
    }

    private static String family(String base) {
        switch (base) {
            case "VARCHAR":
            case "VARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "NVARCHAR":
            case "NVARCHAR2":
            case "STRING":
            case "TEXT":
            case "TINYTEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "CLOB":
            case "LONG":
                return "STRING";
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "LARGEINT":
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "NUMBER":
            case "FLOAT":
            case "DOUBLE":
                return "NUMERIC";
            case "DATE":
            case "DATETIME":
            case "DATETIMEV2":
            case "TIMESTAMP":
            case "TIME":
                return "TIME";
            case "BOOLEAN":
                return "BOOLEAN";
            default:
                return base;
        }
    }

    private static String doris(String type) {
        String base = baseDoris(type);
        switch (base) {
            case "STRING":
                return "STRING";
            case "TEXT":
                return "STRING";
            case "JSON":
                return "JSON";
            case "LARGEINT":
                return "LARGEINT";
            case "INT":
                return "INT";
            case "DATETIME":
                return "DATETIME";
            case "DECIMAL":
                return "DECIMAL" + precision(type);
            default:
                return type;
        }
    }

    private static String mysql(String base, String type) {
        switch (base) {
            case "BOOLEAN":
                return "TINYINT(1)";
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
                return base;
            case "LARGEINT":
                return "DECIMAL(38)";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "DECIMAL":
                return "DECIMAL" + precision(type);
            case "DATE":
                return "DATE";
            case "DATETIME":
            case "TIMESTAMP":
                return "DATETIME";
            case "CHAR":
            case "VARCHAR":
                return type;
            case "STRING":
            case "JSON":
                return "TEXT";
            default:
                return "TEXT";
        }
    }

    private static String oracle(String base, String type) {
        switch (base) {
            case "BOOLEAN":
                return "NUMBER(1)";
            case "TINYINT":
                return "NUMBER(3)";
            case "SMALLINT":
                return "NUMBER(5)";
            case "INT":
                return "NUMBER(10)";
            case "BIGINT":
                return "NUMBER(19)";
            case "LARGEINT":
                return "NUMBER(38)";
            case "FLOAT":
                return "BINARY_FLOAT";
            case "DOUBLE":
                return "BINARY_DOUBLE";
            case "DECIMAL":
                return "NUMBER" + precision(type);
            case "DATE":
                return "DATE";
            case "DATETIME":
            case "TIMESTAMP":
                return "TIMESTAMP";
            case "CHAR":
                return type;
            case "VARCHAR":
                return type.replaceFirst("(?i)^VARCHAR", "VARCHAR2");
            case "STRING":
            case "JSON":
                return "CLOB";
            default:
                return "CLOB";
        }
    }

    private static String dm8(String base, String type) {
        switch (base) {
            case "BOOLEAN":
                return "BIT";
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
                return base;
            case "LARGEINT":
                return "DECIMAL(38)";
            case "FLOAT":
            case "DOUBLE":
                return base;
            case "DECIMAL":
                return type;
            case "DATE":
                return "DATE";
            case "DATETIME":
            case "TIMESTAMP":
                return "TIMESTAMP";
            case "CHAR":
            case "VARCHAR":
                return type;
            case "STRING":
            case "JSON":
                return "TEXT";
            default:
                return "TEXT";
        }
    }

    private static String precision(String type) {
        int idx = type.indexOf('(');
        return idx > 0 ? type.substring(idx) : "(38,10)";
    }

    private static String normalizeDoris(String type) {
        return type == null || type.trim().isEmpty()
                ? "STRING" : type.trim().toUpperCase(Locale.ROOT);
    }

    private static String baseDoris(String type) {
        int paren = type.indexOf('(');
        String base = paren > 0 ? type.substring(0, paren) : type;
        base = base.trim();
        if ("BOOL".equals(base)) {
            return "BOOLEAN";
        }
        if ("INTEGER".equals(base)) {
            return "INT";
        }
        if ("NUMERIC".equals(base) || "NUMBER".equals(base)
                || "DECIMAL".equals(base) || "DECIMALV2".equals(base)
                || "DECIMALV3".equals(base) || "DECIMAL32".equals(base)
                || "DECIMAL64".equals(base) || "DECIMAL128".equals(base)) {
            return "DECIMAL";
        }
        if ("DATETIMEV2".equals(base)) {
            return "DATETIME";
        }
        return base;
    }
}
