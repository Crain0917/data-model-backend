package com.datamodel.sink;

import com.datamodel.sql.Identifiers;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

/**
 * Sink 读取 SQL：显式投影 + 水位线续写谓词 + 稳定排序（架构 M3.7 §18/§19）。
 * 纯函数，便于单测。
 */
public final class SinkSqlPlanner {

    public static final String ROWID_COLUMN = "__dm_rowid";

    private SinkSqlPlanner() {
    }

    public static SinkReadPlan plan(String upstreamTable, SinkConfig config) {
        Identifiers.QuoteStyle quote = Identifiers.QuoteStyle.BACKTICK;
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        for (SinkField field : config.getFieldMapping()) {
            columns.add(field.getSource());
        }
        columns.add(ROWID_COLUMN);

        List<String> refs = new ArrayList<>();
        for (String column : columns) {
            refs.add(Identifiers.quote(column, quote));
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", refs));
        sql.append("\nFROM ").append(Identifiers.quote(upstreamTable, quote));

        String watermark = config.getWatermarkColumn();
        boolean hasWatermark = watermark != null && !watermark.trim().isEmpty();
        if (hasWatermark) {
            sql.append("\nWHERE (").append(predicate(watermark)).append(")");
            sql.append("\nORDER BY ").append(Identifiers.quote(watermark, quote))
                    .append(", ").append(Identifiers.quote(ROWID_COLUMN, quote));
        }
        return new SinkReadPlan(sql.toString(), new ArrayList<>(columns), hasWatermark);
    }

    /** 水位线续写谓词；重复水位值用 __dm_rowid 二次定位。 */
    public static String predicate(String watermarkColumn) {
        String wc = Identifiers.quote(watermarkColumn, Identifiers.QuoteStyle.BACKTICK);
        String rowid = Identifiers.quote(ROWID_COLUMN, Identifiers.QuoteStyle.BACKTICK);
        return wc + " > ? OR (" + wc + " = ? AND " + rowid + " > ?)";
    }

    /** 目标列列表：按映射顺序，不包含内部列。 */
    public static List<String> targetColumns(SinkConfig config) {
        List<String> result = new ArrayList<>();
        for (SinkField field : config.getFieldMapping()) {
            result.add(field.getTarget());
        }
        return result;
    }

    public static boolean isHidden(String column) {
        if (column == null) {
            return false;
        }
        String lower = column.toLowerCase(Locale.ROOT);
        return ROWID_COLUMN.equals(lower) || SinkDdlBuilder.SEQUENCE_COLUMN.equals(lower);
    }

    public static final class SinkReadPlan {
        private final String sql;
        private final List<String> readColumns;
        private final boolean watermark;

        public SinkReadPlan(String sql, List<String> readColumns, boolean watermark) {
            this.sql = sql;
            this.readColumns = readColumns;
            this.watermark = watermark;
        }

        public String getSql() {
            return sql;
        }

        public List<String> getReadColumns() {
            return readColumns;
        }

        public boolean hasWatermark() {
            return watermark;
        }
    }
}
