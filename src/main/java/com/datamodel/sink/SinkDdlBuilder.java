package com.datamodel.sink;

import com.datamodel.sql.Identifiers;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
        List<String> defs = new ArrayList<>();
        if (usesSequenceColumn(targetType)) {
            defs.add(Identifiers.quote(SEQUENCE_COLUMN, quote) + " BIGINT NOT NULL");
        }
        for (SinkField field : config.getFieldMapping()) {
            defs.add(Identifiers.quote(field.getTarget(), quote) + " "
                    + TargetTypeMapper.toTarget(targetType, field.getType()));
        }
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        if (!"ORACLE".equals(targetType) && !"DM8".equals(targetType)) {
            ddl.append("IF NOT EXISTS ");
        }
        ddl.append(qualifiedTable).append(" (\n  ");
        ddl.append(String.join(",\n  ", defs));
        ddl.append("\n)");
        if (usesSequenceColumn(targetType)) {
            String seq = Identifiers.quote(SEQUENCE_COLUMN, quote);
            ddl.append("\nDUPLICATE KEY(").append(seq).append(")");
            ddl.append("\nDISTRIBUTED BY HASH(").append(seq).append(") BUCKETS 1");
            ddl.append("\nPROPERTIES (\"replication_num\" = \"1\")");
        }
        return ddl.toString();
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
