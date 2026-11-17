package com.datamodel.service.connector;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.config.AppProperties;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.dto.ColumnInfo;
import com.datamodel.dto.TableInfo;
import com.datamodel.dto.TestConnectionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

@Component
public class JdbcConnector {

    private static final Logger log = LoggerFactory.getLogger(JdbcConnector.class);
    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z0-9_$]+");

    private final JdbcUrlBuilder urlBuilder;
    private final AppProperties appProperties;

    public JdbcConnector(JdbcUrlBuilder urlBuilder, AppProperties appProperties) {
        this.urlBuilder = urlBuilder;
        this.appProperties = appProperties;
    }

    public Connection openConnection(DataSourceEntity ds, String plainPassword)
            throws SQLException, ClassNotFoundException {
        return openConnection(ds, plainPassword, null);
    }

    /** @param queryTimeoutSecondsOverride 非空时覆盖 JDBC socketTimeout（预览等长查询场景） */
    public Connection openConnection(DataSourceEntity ds, String plainPassword,
                                     Integer queryTimeoutSecondsOverride)
            throws SQLException, ClassNotFoundException {
        Class.forName(DriverResolver.driverClass(ds.getType()));
        Properties props = new Properties();
        props.setProperty("user", ds.getUsername() == null ? "" : ds.getUsername());
        props.setProperty("password", plainPassword == null ? "" : plainPassword);
        return DriverManager.getConnection(urlBuilder.build(ds, queryTimeoutSecondsOverride), props);
    }

    public TestConnectionResult test(DataSourceEntity ds, String plainPassword) {
        long start = System.currentTimeMillis();
        try (Connection connection = openConnection(ds, plainPassword)) {
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(appProperties.getDataSource().getQueryTimeoutSeconds());
                try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                    if (!rs.next()) {
                        throw new SQLException("SELECT 1 未返回结果");
                    }
                }
            }
            if ("DORIS".equalsIgnoreCase(ds.getType())) {
                probeDorisBackend(connection);
            }
            return TestConnectionResult.success(System.currentTimeMillis() - start, "连接成功");
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - start;
            String errorType = ConnectionErrorClassifier.classify(e);
            log.info("数据源连通性测试失败 id={}, errorType={}, msg={}", ds.getId(), errorType, e.getMessage());
            return TestConnectionResult.failure(cost, errorType, ConnectionErrorClassifier.messageOf(errorType, e));
        }
    }

    public List<TableInfo> listTables(Connection connection, DataSourceEntity ds) throws SQLException {
        if ("DM8".equalsIgnoreCase(ds.getType())) {
            return listDmTables(connection, ds.getDefaultDb());
        }
        List<TableInfo> tables = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(appProperties.getDataSource().getQueryTimeoutSeconds());
            try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tables.add(new TableInfo(rs.getString(1)));
                }
            }
        }
        return tables;
    }

    public List<ColumnInfo> listColumns(Connection connection, DataSourceEntity ds, String table) throws SQLException {
        String safeTable = validateTableName(table);
        if ("DM8".equalsIgnoreCase(ds.getType())) {
            return listDmColumns(connection, ds.getDefaultDb(), safeTable);
        }
        String sql = "MYSQL".equalsIgnoreCase(ds.getType())
                ? "SHOW FULL COLUMNS FROM `" + safeTable + "`"
                : "SHOW COLUMNS FROM `" + safeTable + "`";
        List<ColumnInfo> columns = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(appProperties.getDataSource().getQueryTimeoutSeconds());
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    columns.add(new ColumnInfo(rs.getString(1), rs.getString(2)));
                }
            }
        }
        return columns;
    }

    public String validateTableName(String table) {
        if (table == null || !TABLE_NAME.matcher(table).matches()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "表名不合法，仅支持字母、数字、下划线、$");
        }
        return table;
    }

    /** Sink 目标表是否存在（MySQL/Doris 用 catalog，Oracle/DM8 用 schema）。 */
    public boolean tableExists(Connection connection, DataSourceEntity ds, String table) throws SQLException {
        String safeTable = validateTableName(table);
        DatabaseMetaData metaData = connection.getMetaData();
        String catalog = catalogOf(ds);
        for (String schema : candidateSchemas(ds)) {
            for (String candidate : candidateNames(safeTable)) {
                try (ResultSet rs = metaData.getTables(catalog, schema, candidate,
                        new String[]{"TABLE"})) {
                    if (rs.next()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** Sink 目标表列与类型；找不到返回空。 */
    public List<ColumnInfo> describeColumns(Connection connection, DataSourceEntity ds, String table)
            throws SQLException {
        String safeTable = validateTableName(table);
        DatabaseMetaData metaData = connection.getMetaData();
        String catalog = catalogOf(ds);
        for (String schema : candidateSchemas(ds)) {
            for (String candidate : candidateNames(safeTable)) {
                List<ColumnInfo> columns = new ArrayList<>();
                try (ResultSet rs = metaData.getColumns(catalog, schema, candidate, "%")) {
                    while (rs.next()) {
                        String typeName = rs.getString("TYPE_NAME");
                        int size = rs.getInt("COLUMN_SIZE");
                        int digits = rs.getInt("DECIMAL_DIGITS");
                        columns.add(new ColumnInfo(rs.getString("COLUMN_NAME"),
                                formatJdbcType(typeName, size, digits)));
                    }
                }
                if (!columns.isEmpty()) {
                    return columns;
                }
            }
        }
        return new ArrayList<>();
    }

    private static String catalogOf(DataSourceEntity ds) {
        String type = ds.getType() == null ? "" : ds.getType().toUpperCase();
        if ("MYSQL".equals(type) || "DORIS".equals(type)) {
            return ds.getDefaultDb();
        }
        return null;
    }

    private static List<String> candidateSchemas(DataSourceEntity ds) {
        List<String> schemas = new ArrayList<>();
        String type = ds.getType() == null ? "" : ds.getType().toUpperCase();
        String db = ds.getDefaultDb();
        if (db != null && !db.trim().isEmpty()
                && ("ORACLE".equals(type) || "DM8".equals(type))) {
            schemas.add(upper(db));
            schemas.add(db);
        }
        schemas.add(null);
        return schemas;
    }

    private static List<String> candidateNames(String table) {
        List<String> names = new ArrayList<>();
        names.add(table);
        if (!names.contains(table.toUpperCase())) {
            names.add(table.toUpperCase());
        }
        if (!names.contains(table.toLowerCase())) {
            names.add(table.toLowerCase());
        }
        return names;
    }

    private static String formatJdbcType(String typeName, int size, int digits) {
        if (typeName == null) {
            return "";
        }
        String upper = typeName.toUpperCase();
        if ((upper.contains("CHAR") || upper.contains("VARCHAR")) && size > 0) {
            return typeName + "(" + size + ")";
        }
        if ((upper.contains("DECIMAL") || upper.contains("NUMERIC") || upper.equals("NUMBER"))
                && size > 0) {
            return typeName + "(" + size + (digits > 0 ? "," + digits : "") + ")";
        }
        return typeName;
    }

    private List<TableInfo> listDmTables(Connection connection, String schema) throws SQLException {
        List<TableInfo> tables = queryDmTables(connection, upper(schema));
        if (tables.isEmpty() && schema != null) {
            tables = queryDmTables(connection, schema);
        }
        if (tables.isEmpty()) {
            tables = queryDmTables(connection, null);
        }
        return tables;
    }

    private List<TableInfo> queryDmTables(Connection connection, String schema) throws SQLException {
        List<TableInfo> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                tables.add(new TableInfo(rs.getString("TABLE_NAME")));
            }
        }
        return tables;
    }

    private List<ColumnInfo> listDmColumns(Connection connection, String schema, String table) throws SQLException {
        List<ColumnInfo> columns = queryDmColumns(connection, upper(schema), upper(table));
        if (columns.isEmpty() && schema != null) {
            columns = queryDmColumns(connection, schema, table);
        }
        if (columns.isEmpty()) {
            columns = queryDmColumns(connection, null, table);
        }
        return columns;
    }

    private List<ColumnInfo> queryDmColumns(Connection connection, String schema, String table) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getColumns(null, schema, table, "%")) {
            while (rs.next()) {
                String typeName = rs.getString("TYPE_NAME");
                int size = rs.getInt("COLUMN_SIZE");
                int digits = rs.getInt("DECIMAL_DIGITS");
                columns.add(new ColumnInfo(rs.getString("COLUMN_NAME"), formatDmType(typeName, size, digits)));
            }
        }
        return columns;
    }

    private static String formatDmType(String typeName, int size, int digits) {
        if (typeName == null) {
            return "";
        }
        String upper = typeName.toUpperCase();
        if (upper.contains("CHAR") && size > 0) {
            return typeName + "(" + size + ")";
        }
        if ((upper.contains("DECIMAL") || upper.contains("NUMERIC") || upper.equals("NUMBER")) && size > 0) {
            return typeName + "(" + size + (digits > 0 ? "," + digits : "") + ")";
        }
        return typeName;
    }

    private void probeDorisBackend(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(3);
            try (ResultSet rs = statement.executeQuery("SHOW BACKENDS")) {
                while (rs.next()) {
                    // 仅探测 FE 是否可读取 BE 列表，失败不影响 SELECT 1 结果
                }
            }
        } catch (Exception e) {
            log.debug("Doris BACKENDS 探测失败（不阻断连通性结果）：{}", e.getMessage());
        }
    }

    private static String upper(String value) {
        return value == null ? null : value.toUpperCase();
    }
}
