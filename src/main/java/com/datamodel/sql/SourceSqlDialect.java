package com.datamodel.sql;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Locale;

/**
 * 源库方言的标识符引用与参数绑定。同步与数据预览共用，避免两套拼装逻辑。
 */
public final class SourceSqlDialect {

    private SourceSqlDialect() {
    }

    public static Identifiers.QuoteStyle quoteStyle(String dialect) {
        String d = dialect == null ? "" : dialect.trim().toUpperCase(Locale.ROOT);
        return "DM8".equals(d) || "ORACLE".equals(d)
                ? Identifiers.QuoteStyle.DOUBLE
                : Identifiers.QuoteStyle.BACKTICK;
    }

    public static String tableRef(String dialect, String schema, String table) {
        Identifiers.QuoteStyle style = quoteStyle(dialect);
        String t = Identifiers.quote(Identifiers.requireField(table), style);
        if (schema == null || schema.trim().isEmpty()) {
            return t;
        }
        return Identifiers.quote(Identifiers.requireField(schema), style) + "." + t;
    }

    public static String columnRef(String dialect, String column) {
        return Identifiers.quote(Identifiers.requireField(column), quoteStyle(dialect));
    }

    public static void bind(PreparedStatement ps, List<Object> params) throws Exception {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }
}
