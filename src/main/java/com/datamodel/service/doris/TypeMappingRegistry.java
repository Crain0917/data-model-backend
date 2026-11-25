package com.datamodel.service.doris;

import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * MySQL / Oracle / DM8 → Doris 4.1.3 类型映射。
 * 规则固化自《数据同步与物理化设计-v1》§3；未命中一律 STRING + WARN。
 */
@Component
public class TypeMappingRegistry {

    public static final String MYSQL = "MYSQL";
    public static final String ORACLE = "ORACLE";
    public static final String DM8 = "DM8";
    public static final String DORIS = "DORIS";

    /** 已是 Doris 合法类型、无需再映射的基类型；其余一律走源库映射规则。 */
    private static final java.util.Set<String> DORIS_PASSTHROUGH = new java.util.HashSet<>(java.util.Arrays.asList(
            "BOOLEAN", "LARGEINT", "STRING", "JSON", "DATETIMEV2"
    ));

    /**
     * 已经是 Doris 合法类型名的基类型。命中时直接按 Doris 规则归一（幂等），
     * 避免把 LARGEINT / DATETIMEV2 之类再按 MySQL 规则映射成 STRING。
     */
    private static final java.util.Set<String> DORIS_NATIVE_BASES = new java.util.HashSet<>(java.util.Arrays.asList(
            "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT", "FLOAT", "DOUBLE",
            "DECIMAL", "CHAR", "VARCHAR", "STRING", "DATE", "DATETIME", "DATETIMEV2", "DATEV2",
            "JSON", "ARRAY", "MAP", "STRUCT", "VARIANT", "HLL", "BITMAP", "IPV4", "IPV6"
    ));

    /**
     * 按数据源方言归一化字段类型：源库原始类型（bigint unsigned / VARCHAR2 / NUMBER(10,2)）
     * 走 dialect 映射；已是 Doris 类型的值保持幂等，不会二次降级。
     */
    public TypeMapping normalize(String dialect, String rawType) {
        if (rawType == null || rawType.trim().isEmpty()) {
            return TypeMapping.fallback("STRING", "字段类型为空，已按 STRING 兜底");
        }
        Parsed p = Parsed.of(rawType);
        if (!p.unsigned && DORIS_NATIVE_BASES.contains(p.base)) {
            return normalizeDorisType(rawType);
        }
        String d = dialect == null ? null : dialect.trim().toUpperCase(Locale.ROOT);
        if (d == null || d.isEmpty() || DORIS.equals(d)) {
            return normalizeDorisType(rawType);
        }
        return map(d, rawType);
    }

    /**
     * 中间表 / DDL 用类型归一化，幂等：输入源库原始类型（{@code bigint unsigned}、{@code datetime(3)}）
     * 或已是 Doris 类型（{@code LARGEINT}、{@code DATETIMEV2(3)}）都归一为合法 Doris 类型。
     * 无符号整型必须加宽，DDL 中不得出现 UNSIGNED。
     */
    public TypeMapping normalizeDorisType(String rawType) {
        if (rawType == null || rawType.trim().isEmpty()) {
            return TypeMapping.fallback("STRING", "字段类型为空，已按 STRING 兜底");
        }
        Parsed p = Parsed.of(rawType);
        if (!p.unsigned && DORIS_PASSTHROUGH.contains(p.base)) {
            if ("DATETIMEV2".equals(p.base) && p.param(0, 0) > 6) {
                return TypeMapping.of("DATETIMEV2(6)", "DATETIMEV2 精度超过 6，已截断为 6");
            }
            return TypeMapping.of(p.original);
        }
        // 无符号整型、TIMESTAMP->DATETIME、TIME/ENUM/SET/BLOB->STRING、未知->STRING 均复用 MySQL 规则
        return mapMysql(p);
    }

    public TypeMapping map(String dialect, String rawType) {
        if (rawType == null || rawType.trim().isEmpty()) {
            return TypeMapping.fallback("STRING", "字段类型为空，已按 STRING 兜底");
        }
        Parsed p = Parsed.of(rawType);
        String d = dialect == null ? MYSQL : dialect.trim().toUpperCase(Locale.ROOT);
        switch (d) {
            case MYSQL:
                return mapMysql(p);
            case ORACLE:
                return mapOracle(p);
            case DM8:
                return mapDm(p);
            case DORIS:
                return TypeMapping.of(p.original);
            default:
                return mapMysql(p);
        }
    }

    // ---------------------------------------------------------------- MySQL

    private TypeMapping mapMysql(Parsed p) {
        String base = p.base;
        switch (base) {
            case "TINYINT":
                return p.unsigned ? TypeMapping.of("SMALLINT") : TypeMapping.of("TINYINT");
            case "SMALLINT":
                return p.unsigned ? TypeMapping.of("INT") : TypeMapping.of("SMALLINT");
            case "MEDIUMINT":
                return TypeMapping.of("INT");
            case "INT":
            case "INTEGER":
                return p.unsigned ? TypeMapping.of("BIGINT") : TypeMapping.of("INT");
            case "BIGINT":
                return p.unsigned ? TypeMapping.of("LARGEINT") : TypeMapping.of("BIGINT");
            case "DECIMAL":
            case "NUMERIC":
            case "DEC":
                return decimal(p, null);
            case "FLOAT":
                return TypeMapping.of("FLOAT");
            case "DOUBLE":
            case "REAL":
                return TypeMapping.of("DOUBLE");
            case "CHAR":
                return charType(p);
            case "VARCHAR":
            case "CHARACTER VARYING":
                return varcharType(p, 65533);
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                return TypeMapping.of("STRING");
            case "DATE":
                return TypeMapping.of("DATE");
            case "DATETIME":
                return p.fsp() > 0 ? TypeMapping.of("DATETIMEV2(" + p.fsp() + ")") : TypeMapping.of("DATETIME");
            case "TIMESTAMP":
                return TypeMapping.of("DATETIME", "TIMESTAMP 按源会话时区落值，时区信息丢弃");
            case "TIME":
                return TypeMapping.of("STRING", "Doris 无 TIME 类型，按 STRING 存储");
            case "YEAR":
                return TypeMapping.of("SMALLINT");
            case "ENUM":
                return TypeMapping.of("STRING", "ENUM 按 label 文本存储");
            case "SET":
                return TypeMapping.of("STRING", "SET 按逗号拼接文本存储");
            case "BIT":
                return bitType(p);
            case "JSON":
                return TypeMapping.of("JSON");
            case "GEOMETRY":
            case "POINT":
            case "LINESTRING":
            case "POLYGON":
                return TypeMapping.of("STRING", "空间类型 v1 不可用，按 STRING 存储");
            default:
                return TypeMapping.fallback("STRING", "未识别的 MySQL 类型 " + p.original + "，已按 STRING 兜底");
        }
    }

    // --------------------------------------------------------------- Oracle

    private TypeMapping mapOracle(Parsed p) {
        String base = p.base;
        switch (base) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                if (!p.hasParams()) {
                    return TypeMapping.of("DOUBLE", "NUMBER 未指定精度，按 DOUBLE 映射，精度可能丢失");
                }
                return decimal(p, null);
            case "INTEGER":
            case "INT":
            case "SMALLINT":
                return TypeMapping.of("DECIMAL(38,0)", "Oracle 整数类型统一按 NUMBER 语义映射");
            case "FLOAT":
            case "BINARY_FLOAT":
                return TypeMapping.of("FLOAT");
            case "BINARY_DOUBLE":
                return TypeMapping.of("DOUBLE");
            case "CHAR":
            case "NCHAR":
                return charType(p);
            case "VARCHAR2":
            case "NVARCHAR2":
            case "VARCHAR":
                return varcharType(p, 65533);
            case "CLOB":
            case "NCLOB":
            case "LONG":
            case "BLOB":
            case "RAW":
            case "LONG RAW":
                return TypeMapping.of("STRING");
            case "DATE":
                return TypeMapping.of("DATETIME");
            case "TIMESTAMP":
                if (p.original.contains("TIME ZONE")) {
                    return TypeMapping.of("DATETIMEV2(" + (p.fsp() > 0 ? p.fsp() : 0) + ")",
                            "TIMESTAMP WITH TIME ZONE 丢弃时区");
                }
                return TypeMapping.of("DATETIMEV2(" + (p.fsp() > 0 ? p.fsp() : 0) + ")");
            case "INTERVAL":
                return TypeMapping.of("STRING", "INTERVAL 类型按 STRING 存储");
            case "ROWID":
            case "UROWID":
                return TypeMapping.of("STRING");
            case "BOOLEAN":
                return TypeMapping.of("BOOLEAN");
            default:
                return TypeMapping.fallback("STRING", "未识别的 Oracle 类型 " + p.original + "，已按 STRING 兜底");
        }
    }

    // ----------------------------------------------------------------- DM8

    private TypeMapping mapDm(Parsed p) {
        String base = p.base;
        switch (base) {
            case "TINYINT":
                return TypeMapping.of("TINYINT");
            case "SMALLINT":
                return TypeMapping.of("SMALLINT");
            case "INT":
            case "INTEGER":
                return TypeMapping.of("INT");
            case "BIGINT":
                return TypeMapping.of("BIGINT");
            case "DECIMAL":
            case "NUMERIC":
            case "NUMBER":
                if (!p.hasParams()) {
                    return TypeMapping.of("DOUBLE", "NUMBER 未指定精度，按 DOUBLE 映射，精度可能丢失");
                }
                return decimal(p, null);
            case "FLOAT":
                return TypeMapping.of("DOUBLE", "DM8 FLOAT 为近似类型，按 DOUBLE 映射");
            case "DOUBLE":
            case "REAL":
                return TypeMapping.of("DOUBLE");
            case "CHAR":
            case "CHARACTER":
                return charType(p);
            case "VARCHAR":
            case "VARCHAR2":
                return varcharType(p, 65533);
            case "TEXT":
            case "CLOB":
            case "BLOB":
            case "BINARY":
            case "VARBINARY":
                return TypeMapping.of("STRING");
            case "DATE":
                return TypeMapping.of("DATE");
            case "TIME":
                return TypeMapping.of("STRING", "Doris 无 TIME 类型，按 STRING 存储");
            case "DATETIME":
            case "TIMESTAMP":
                return TypeMapping.of("DATETIME");
            case "BIT":
            case "BOOLEAN":
                return TypeMapping.of("BOOLEAN");
            default:
                return TypeMapping.fallback("STRING", "未识别的 DM8 类型 " + p.original + "，已按 STRING 兜底");
        }
    }

    // ------------------------------------------------------------- helpers

    private TypeMapping decimal(Parsed p, String unused) {
        int precision = p.param(0, -1);
        int scale = p.param(1, 0);
        if (precision <= 0) {
            return TypeMapping.fallback("STRING", "DECIMAL 未指定精度，已按 STRING 兜底");
        }
        if (precision > 38) {
            return TypeMapping.fallback("STRING", "DECIMAL 精度 " + precision + " 超过 Doris 上限 38，已按 STRING 兜底");
        }
        return TypeMapping.of("DECIMAL(" + precision + "," + Math.max(0, scale) + ")");
    }

    private TypeMapping charType(Parsed p) {
        int len = p.param(0, -1);
        if (len <= 0) {
            return TypeMapping.of("STRING", "CHAR 未指定长度，已按 STRING 兜底");
        }
        if (len <= 255) {
            return TypeMapping.of("CHAR(" + len + ")");
        }
        if (len <= 65533) {
            return TypeMapping.of("VARCHAR(" + len + ")", "CHAR 长度超过 255，按 VARCHAR 映射");
        }
        return TypeMapping.fallback("STRING", "CHAR 长度 " + len + " 超过 65533，已按 STRING 兜底");
    }

    private TypeMapping varcharType(Parsed p, int max) {
        int len = p.param(0, -1);
        if (len <= 0) {
            return TypeMapping.of("STRING", "VARCHAR 未指定长度，已按 STRING 兜底");
        }
        if (len <= max) {
            return TypeMapping.of("VARCHAR(" + len + ")");
        }
        return TypeMapping.fallback("STRING", "VARCHAR 长度 " + len + " 超过 " + max + "，已按 STRING 兜底");
    }

    private TypeMapping bitType(Parsed p) {
        int n = p.param(0, 1);
        if (n <= 1) {
            return TypeMapping.of("BOOLEAN");
        }
        if (n <= 64) {
            return TypeMapping.of("BIGINT");
        }
        return TypeMapping.fallback("STRING", "BIT(" + n + ") 超过 64 位，已按 STRING 兜底");
    }

    /** 解析 "VARCHAR(64)" / "NUMBER(10,2)" / "BIGINT UNSIGNED"。 */
    static final class Parsed {
        final String original;
        final String base;
        final String params;
        final boolean unsigned;

        private Parsed(String original, String base, String params, boolean unsigned) {
            this.original = original;
            this.base = base;
            this.params = params;
            this.unsigned = unsigned;
        }

        static Parsed of(String raw) {
            String original = raw.trim().replaceAll("\\s+", " ");
            String upper = original.toUpperCase(Locale.ROOT);
            String work = upper;
            int paren = work.indexOf('(');
            String basePart = paren >= 0 ? work.substring(0, paren).trim() : work.trim();
            String params = "";
            if (paren >= 0) {
                int close = work.indexOf(')', paren);
                if (close > paren) {
                    params = work.substring(paren + 1, close).trim();
                }
            }
            boolean unsigned = basePart.endsWith(" UNSIGNED") || basePart.contains(" UNSIGNED ");
            if (basePart.endsWith(" UNSIGNED")) {
                basePart = basePart.substring(0, basePart.length() - " UNSIGNED".length()).trim();
            }
            // TIMESTAMP WITH [LOCAL] TIME ZONE 统一归到 TIMESTAMP，由调用方读 original 判断时区
            if (basePart.startsWith("TIMESTAMP")) {
                basePart = "TIMESTAMP";
            }
            if (basePart.startsWith("INTERVAL")) {
                basePart = "INTERVAL";
            }
            if (basePart.startsWith("DOUBLE")) {
                basePart = "DOUBLE";
            }
            return new Parsed(original, basePart, params, unsigned);
        }

        boolean hasParams() {
            return !params.isEmpty();
        }

        int param(int index, int defaultValue) {
            if (params.isEmpty()) {
                return defaultValue;
            }
            String[] parts = params.split(",");
            if (index >= parts.length) {
                return defaultValue;
            }
            try {
                return Integer.parseInt(parts[index].trim());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }

        int fsp() {
            int v = param(0, 0);
            if (v <= 0) {
                return 0;
            }
            return Math.min(v, 6);
        }
    }
}
