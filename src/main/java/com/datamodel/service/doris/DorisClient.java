package com.datamodel.service.doris;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.config.DorisProperties;
import com.datamodel.domain.DataSourceEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Doris FE 访问层：JDBC（9030）执行 DDL/DML，HTTP（8040）走 Stream Load。
 */
@Component
public class DorisClient {

    private static final Logger log = LoggerFactory.getLogger(DorisClient.class);
    private static final Pattern KEY_MODEL = Pattern.compile(
            "\\b(UNIQUE|DUPLICATE|AGGREGATE)\\s+KEY\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);

    private final DorisProperties props;
    private final ObjectMapper objectMapper;

    public DorisClient(DorisProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
    }

    /**
     * 一个 Doris 端点：Sink 目标可指向用户配置的 Doris 数据源，
     * 此时 JDBC 查询端口与 Stream Load HTTP 端口、库名、账号均来自该数据源。
     */
    public static final class DorisEndpoint {
        private final String name;
        private final String host;
        private final int queryPort;
        private final int httpPort;
        private final String user;
        private final String password;
        private final String database;

        public DorisEndpoint(String name, String host, int queryPort, int httpPort,
                             String user, String password, String database) {
            this.name = name;
            this.host = host;
            this.queryPort = queryPort;
            this.httpPort = httpPort;
            this.user = user;
            this.password = password;
            this.database = database;
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        public int getQueryPort() {
            return queryPort;
        }

        public int getHttpPort() {
            return httpPort;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public String getDatabase() {
            return database;
        }

        /** 日志标识：DORIS[dsName]@host:httpPort:db（不含口令）。 */
        public String describe() {
            String label = name == null || name.trim().isEmpty() ? "platform" : name.trim();
            return "DORIS[" + label + "]@" + host + ":" + httpPort + ":" + database;
        }
    }

    /** SHOW CREATE TABLE 解析结果：数据模型与 KEY 列（保持表内顺序）。 */
    public static final class TableMeta {
        private final String model;
        private final List<String> keyColumns;

        TableMeta(String model, List<String> keyColumns) {
            this.model = model;
            this.keyColumns = keyColumns;
        }

        public String getModel() {
            return model;
        }

        public List<String> getKeyColumns() {
            return keyColumns;
        }
    }

    /** 平台内置 Doris（上游物化表所在库）。 */
    public DorisEndpoint platformEndpoint() {
        return new DorisEndpoint("platform", props.getHost(), props.getQueryPort(),
                props.getHttpPort(), props.getUser(), props.getPassword(), props.getSchema());
    }

    /**
     * 由用户配置的 Doris 数据源构造端点。HTTP 端口解析顺序：
     * extraParams.httpPort → 与平台地址完全一致时用平台 httpPort → queryPort-1000。
     */
    public DorisEndpoint endpointFor(DataSourceEntity ds, String plainPassword) {
        String host = ds.getHost();
        int queryPort = ds.getPort() == null ? props.getQueryPort() : ds.getPort();
        String database = ds.getDefaultDb() == null || ds.getDefaultDb().trim().isEmpty()
                ? props.getSchema() : ds.getDefaultDb().trim();
        Integer httpPort = extraHttpPort(ds.getExtraParams());
        if (httpPort == null) {
            if (host != null && host.equals(props.getHost()) && queryPort == props.getQueryPort()) {
                httpPort = props.getHttpPort();
            } else {
                httpPort = queryPort > 1000 ? queryPort - 1000 : 8040;
            }
        }
        return new DorisEndpoint(ds.getName(), host, queryPort, httpPort,
                ds.getUsername() == null ? "" : ds.getUsername(),
                plainPassword == null ? "" : plainPassword, database);
    }

    private Integer extraHttpPort(String extraParams) {
        if (extraParams == null || extraParams.trim().isEmpty()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(extraParams);
            JsonNode value = node == null ? null : node.get("httpPort");
            if (value != null && !value.isNull()) {
                int port = value.isInt() ? value.asInt() : Integer.parseInt(value.asText().trim());
                return port > 0 ? port : null;
            }
        } catch (Exception e) {
            log.warn("解析数据源 extraParams.httpPort 失败，按默认端口推导：{}", e.getMessage());
        }
        return null;
    }

    public void requireConfigured() {
        if (!props.isConfigured()) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "Doris 未配置：请设置 DORIS_USER / DORIS_PASSWORD");
        }
    }

    public String qualified(String table) {
        return props.getSchema() + "." + table;
    }

    public String qualifiedQuoted(String table) {
        return qualifiedQuoted(platformEndpoint(), table);
    }

    public String qualifiedQuoted(DorisEndpoint endpoint, String table) {
        return "`" + endpoint.getDatabase().replace("`", "``") + "`."
                + "`" + table.replace("`", "``") + "`";
    }

    public Connection open() throws SQLException {
        return open(platformEndpoint());
    }

    public Connection open(DorisEndpoint endpoint) throws SQLException {
        requireConfigured();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "MySQL/Doris 驱动未加载");
        }
        String url = "jdbc:mysql://" + endpoint.getHost() + ":" + endpoint.getQueryPort() + "/"
                + endpoint.getDatabase() + "?useUnicode=true&characterEncoding=utf8&useSSL=false"
                + "&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai&connectTimeout=5000";
        return DriverManager.getConnection(url, endpoint.getUser(), endpoint.getPassword());
    }

    public boolean tableExists(String table) {
        return tableExists(platformEndpoint(), table);
    }

    public boolean tableExists(DorisEndpoint endpoint, String table) {
        String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (Connection c = open(endpoint); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, endpoint.getDatabase());
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getLong(1) > 0;
            }
        } catch (SQLException e) {
            log.warn("查询 Doris 表是否存在失败 table={}: {}", table, e.getMessage());
            return false;
        }
    }

    /** 与 tableExists 相同，但 Doris 不可达时抛出异常，供删除任务显式回报失败。 */
    public boolean tableExistsStrict(String table) {
        return tableExistsStrict(platformEndpoint(), table);
    }

    public boolean tableExistsStrict(DorisEndpoint endpoint, String table) {
        String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (Connection c = open(endpoint); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, endpoint.getDatabase());
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                    "查询 Doris 表是否存在失败：" + safeMessage(e));
        }
    }

    /** 目标/上游表列与类型（Doris information_schema）。 */
    public List<com.datamodel.dto.ColumnInfo> listColumns(String table) {
        return listColumns(platformEndpoint(), table);
    }

    public List<com.datamodel.dto.ColumnInfo> listColumns(DorisEndpoint endpoint, String table) {
        String sql = "SELECT column_name, column_type FROM information_schema.columns"
                + " WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        List<com.datamodel.dto.ColumnInfo> columns = new java.util.ArrayList<>();
        try (Connection c = open(endpoint); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, endpoint.getDatabase());
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(new com.datamodel.dto.ColumnInfo(rs.getString(1), rs.getString(2)));
                }
            }
            return columns;
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                    "查询 Doris 表结构失败：" + safeMessage(e));
        }
    }

    /** 读取目标表实际建表模型与 KEY 列，用于配置与已有表的一致性校验。 */
    public TableMeta describeTableModel(DorisEndpoint endpoint, String table) {
        String sql = "SHOW CREATE TABLE " + qualifiedQuoted(endpoint, table);
        String ddl;
        try (Connection c = open(endpoint); Statement st = c.createStatement()) {
            st.setQueryTimeout(props.getTimeoutSeconds());
            try (ResultSet rs = st.executeQuery(sql)) {
                ddl = rs.next() ? rs.getString(2) : "";
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                    "读取 Doris 表模型失败：" + safeMessage(e));
        }
        return parseTableMeta(ddl);
    }

    static TableMeta parseTableMeta(String ddl) {
        if (ddl != null) {
            Matcher matcher = KEY_MODEL.matcher(ddl);
            if (matcher.find()) {
                List<String> keys = new ArrayList<>();
                for (String part : matcher.group(2).split(",")) {
                    String key = part.replace("`", "").replace("\"", "").trim();
                    if (!key.isEmpty()) {
                        keys.add(key);
                    }
                }
                return new TableMeta(matcher.group(1).toUpperCase(Locale.ROOT), keys);
            }
        }
        return new TableMeta(null, new ArrayList<>());
    }

    /** 平台 Doris 中指定前缀的表名（用于保存画布时对账清理 dm_mid_*）。 */
    public List<String> listTablesWithPrefix(String prefix) {
        String sql = "SELECT table_name FROM information_schema.tables"
                + " WHERE table_schema = ? AND table_name LIKE ? ESCAPE '\\\\' ORDER BY table_name";
        String escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%";
        List<String> tables = new java.util.ArrayList<>();
        try (Connection c = open(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, props.getSchema());
            ps.setString(2, escaped);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            return tables;
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                    "查询 Doris 表清单失败：" + safeMessage(e));
        }
    }

    public String schema() {
        return props.getSchema();
    }

    public void execute(String sql) {
        execute(platformEndpoint(), sql);
    }

    public void execute(DorisEndpoint endpoint, String sql) {
        try (Connection c = open(endpoint); Statement st = c.createStatement()) {
            st.setQueryTimeout(props.getTimeoutSeconds());
            st.execute(sql);
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED, "Doris 执行失败：" + safeMessage(e));
        }
    }

    public void executePrepared(String sql, List<Object> params) {
        try (Connection c = open(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setQueryTimeout(props.getTimeoutSeconds());
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.execute();
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED, "Doris 执行失败：" + safeMessage(e));
        }
    }

    public long queryLong(String sql) {
        try (Connection c = open(); Statement st = c.createStatement()) {
            st.setQueryTimeout(props.getTimeoutSeconds());
            try (ResultSet rs = st.executeQuery(sql)) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED, "Doris 查询失败：" + safeMessage(e));
        }
    }

    public void truncate(String table) {
        truncate(platformEndpoint(), table);
    }

    public void truncate(DorisEndpoint endpoint, String table) {
        execute(endpoint, "TRUNCATE TABLE " + qualifiedQuoted(endpoint, table));
    }

    public void dropTable(String table) {
        execute("DROP TABLE IF EXISTS " + qualified(table));
    }

    public long tableRowCount(String table) {
        return queryLong("SELECT COUNT(*) FROM " + qualified(table));
    }

    /**
     * 单批 Stream Load。返回 Success / Publish Timeout / Label Already Exists 均视为幂等成功。
     */
    public StreamLoadResult streamLoad(String table, byte[] body, String label,
                                       List<String> jsonPaths, int timeoutSeconds) {
        return streamLoad(platformEndpoint(), table, body, label, jsonPaths, timeoutSeconds);
    }

    public StreamLoadResult streamLoad(DorisEndpoint endpoint, String table, byte[] body,
                                       String label, List<String> jsonPaths, int timeoutSeconds) {
        requireConfigured();
        long start = System.currentTimeMillis();
        HttpURLConnection conn = null;
        try {
            String url = "http://" + endpoint.getHost() + ":" + endpoint.getHttpPort()
                    + "/api/" + endpoint.getDatabase() + "/" + table + "/_stream_load";
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(Math.max(30, timeoutSeconds) * 1000);
            conn.setRequestProperty("Authorization", "Basic " + basicAuth(endpoint));
            conn.setRequestProperty("format", "json");
            conn.setRequestProperty("read_json_by_line", "true");
            conn.setRequestProperty("jsonpaths", objectMapper.writeValueAsString(jsonPaths));
            conn.setRequestProperty("max_filter_ratio", "0");
            conn.setRequestProperty("strict_mode", "true");
            conn.setRequestProperty("timeout", String.valueOf(timeoutSeconds));
            conn.setRequestProperty("label", label);
            conn.setRequestProperty("Expect", "100-continue");
            conn.setFixedLengthStreamingMode(body.length);
            try (OutputStream out = conn.getOutputStream()) {
                out.write(body);
                out.flush();
            }
            int httpCode = conn.getResponseCode();
            String response = readBody(httpCode >= 400 ? conn.getErrorStream() : conn.getInputStream());
            return parseResponse(httpCode, response, start);
        } catch (IOException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED, "Stream Load 请求失败：" + safeMessage(e));
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private StreamLoadResult parseResponse(int httpCode, String response, long start) {
        long cost = System.currentTimeMillis() - start;
        try {
            JsonNode json = objectMapper.readTree(response == null ? "{}" : response);
            String status = json.path("Status").asText("");
            String message = json.path("Message").asText("");
            long loaded = json.path("NumberLoadedRows").asLong(0);
            long loadMs = json.path("LoadTimeMs").asLong(cost);
            boolean success = "Success".equalsIgnoreCase(status)
                    || "Publish Timeout".equalsIgnoreCase(status)
                    || message.contains("Label Already Exists");
            if (!success) {
                String errorUrl = json.path("ErrorURL").asText("");
                String detail = "Stream Load " + status + "：" + message
                        + (errorUrl.isEmpty() ? "" : "，错误详情：" + errorUrl);
                throw new BizException(ErrorCode.DORIS_EXEC_FAILED, detail);
            }
            return new StreamLoadResult(true, loaded, status, message, loadMs);
        } catch (IOException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                    "Stream Load 响应解析失败（HTTP " + httpCode + "）");
        }
    }

    private String basicAuth(DorisEndpoint endpoint) {
        String token = endpoint.getUser() + ":" + endpoint.getPassword();
        return Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }

    private static String readBody(InputStream in) throws IOException {
        if (in == null) {
            return "";
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) {
            out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String safeMessage(Exception e) {
        String m = e.getMessage();
        if (m == null) {
            return e.getClass().getSimpleName();
        }
        return m.length() > 500 ? m.substring(0, 500) : m;
    }
}
