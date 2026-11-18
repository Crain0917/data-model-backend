package com.datamodel.sink;

import com.datamodel.sql.Identifiers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Sink 自动建表 DDL（架构 M3.7 §20）。
 * Doris 目标表注入 __dm_sink_seq 作为唯一键；其他库按字段原样建表。
 */
public final class SinkDdlBuilder {

    public static final String SEQUENCE_COLUMN = "__dm_sink_seq";

    private SinkDdlBuilder() {
    }

    public static boolean usesSequenceColumn(String targetType) {
        return SinkConfig.DORIS.equalsIgnoreCase(targetType);
    }

    /** Doris UNIQUE 模型不注入隐藏序列键；Duplicate 模型仍以序列键保证可重复追加。 */
    public static boolean usesSequenceColumn(SinkConfig config) {
        return usesSequenceColumn(config.getTargetType()) && !config.isUniqueModel();
    }

    public static String buildCreateTable(SinkConfig config) {
        return buildCreateTable(config, Identifiers.quote(config.getTableName(),
                quoteStyle(config.getTargetType())));
    }

    public static String buildCreateTable(SinkConfig config, String qualifiedTable) {
        if (config.getFieldMapping().isEmpty()) {
            throw new IllegalArgumentException("字段映射为空，无法建表");
        }
        String targetType = config.getTargetType().toUpperCase(Locale.ROOT);
        Identifiers.QuoteStyle quote = quoteStyle(targetType);
        boolean doris = usesSequenceColumn(targetType);
        boolean unique = doris && config.isUniqueModel();
        List<String> keys = unique ? uniqueKeys(config) : new ArrayList<>();
        List<String> defs = new ArrayList<>();
        if (unique) {
            // UNIQUE KEY 必须先定义且非空，其余列按映射顺序显式 NULL。
            Map<String, SinkField> byTarget = new LinkedHashMap<>();
            for (SinkField field : config.getFieldMapping()) {
                byTarget.put(field.getTarget().toLowerCase(Locale.ROOT), field);
            }
            for (String key : keys) {
                SinkField field = byTarget.get(key.toLowerCase(Locale.ROOT));
                defs.add(Identifiers.quote(field.getTarget(), quote) + " "
                        + TargetTypeMapper.toTarget(targetType, field.getType()) + " NOT NULL");
            }
            for (SinkField field : config.getFieldMapping()) {
                if (!keys.contains(field.getTarget())) {
                    defs.add(Identifiers.quote(field.getTarget(), quote) + " "
                            + TargetTypeMapper.toTarget(targetType, field.getType()) + " NULL");
                }
            }
        } else {
            if (doris) {
                defs.add(Identifiers.quote(SEQUENCE_COLUMN, quote) + " BIGINT NOT NULL");
            }
            for (SinkField field : config.getFieldMapping()) {
                defs.add(Identifiers.quote(field.getTarget(), quote) + " "
                        + TargetTypeMapper.toTarget(targetType, field.getType()));
            }
        }
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        if (!"ORACLE".equals(targetType) && !"DM8".equals(targetType)) {
            ddl.append("IF NOT EXISTS ");
        }
        ddl.append(qualifiedTable).append(" (\n  ");
        ddl.append(String.join(",\n  ", defs));
        ddl.append("\n)");
        if (unique) {
            List<String> quotedKeys = new ArrayList<>();
            for (String key : keys) {
                quotedKeys.add(Identifiers.quote(key, quote));
            }
            ddl.append("\nUNIQUE KEY(").append(String.join(", ", quotedKeys)).append(")");
            ddl.append("\nDISTRIBUTED BY HASH(").append(quotedKeys.get(0)).append(") BUCKETS 1");
            ddl.append("\nPROPERTIES (\"replication_num\" = \"1\")");
        } else if (doris) {
            String seq = Identifiers.quote(SEQUENCE_COLUMN, quote);
            ddl.append("\nDUPLICATE KEY(").append(seq).append(")");
            ddl.append("\nDISTRIBUTED BY HASH(").append(seq).append(") BUCKETS 1");
            ddl.append("\nPROPERTIES (\"replication_num\" = \"1\")");
        }
        return ddl.toString();
    }

    /** 主键按映射目标列归一（大小写），未出现在映射中的键无法建表。 */
    private static List<String> uniqueKeys(SinkConfig config) {
        Map<String, String> targetNames = new LinkedHashMap<>();
        for (SinkField field : config.getFieldMapping()) {
            targetNames.put(field.getTarget().toLowerCase(Locale.ROOT), field.getTarget());
        }
        List<String> keys = new ArrayList<>();
        for (String key : config.getKeyColumns()) {
            String actual = targetNames.get(key.toLowerCase(Locale.ROOT));
            if (actual == null) {
                throw new IllegalArgumentException("主键字段不在输出字段中：" + key);
            }
            if (keys.contains(actual)) {
                throw new IllegalArgumentException("主键字段重复：" + actual);
            }
            keys.add(actual);
        }
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Unique Key 模型至少需要一个主键字段");
        }
        return keys;
    }

    public static Identifiers.QuoteStyle quoteStyle(String targetType) {
        String type = targetType == null ? SinkConfig.DORIS : targetType.toUpperCase(Locale.ROOT);
        switch (type) {
            case "MYSQL":
                return Identifiers.QuoteStyle.BACKTICK;
            case "ORACLE":
            case "DM8":
                return Identifiers.QuoteStyle.DOUBLE;
            default:
                return Identifiers.QuoteStyle.BACKTICK;
        }
    }

    public static String qualified(String targetType, String tableName) {
        return Identifiers.quote(tableName, quoteStyle(targetType));
    }

    public static String column(String targetType, String name) {
        return Identifiers.quote(name, quoteStyle(targetType));
    }
}
