package com.datamodel.service;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.crypto.AesGcmCipher;
import com.datamodel.config.AppProperties;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.domain.FlowEntity;
import com.datamodel.dto.ColumnInfo;
import com.datamodel.dto.NodeDataPreviewRequest;
import com.datamodel.dto.NodeDataPreviewResponse;
import com.datamodel.service.connector.JdbcConnector;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.datamodel.sql.Condition;
import com.datamodel.sql.ConditionCompiler;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.Identifiers;
import com.datamodel.sql.SourceSqlDialect;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 节点数据预览（M3.6 §10）：Source 过滤下推源库，其余节点查已物化 Doris 表。
 * 只读、不触发执行；sql + params 唯一可执行。
 */
@Service
public class NodeDataService {

    private static final Logger log = LoggerFactory.getLogger(NodeDataService.class);
    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 200;
    private static final int CELL_MAX_BYTES = 32 * 1024;
    private static final String HIDDEN_COLUMN = "__dm_rowid";
    private static final String TRUNCATED_MARK = "…(截断)";

    private final FlowService flowService;
    private final DataSourceService dataSourceService;
    private final AesGcmCipher aesGcmCipher;
    private final JdbcConnector jdbcConnector;
    private final AppProperties appProperties;
    private final DorisClient dorisClient;
    private final DorisNameSanitizer nameSanitizer;

    public NodeDataService(FlowService flowService,
                           DataSourceService dataSourceService,
                           AesGcmCipher aesGcmCipher,
                           JdbcConnector jdbcConnector,
                           AppProperties appProperties,
                           DorisClient dorisClient,
                           DorisNameSanitizer nameSanitizer) {
        this.flowService = flowService;
        this.dataSourceService = dataSourceService;
        this.aesGcmCipher = aesGcmCipher;
        this.jdbcConnector = jdbcConnector;
        this.appProperties = appProperties;
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
    }

    public NodeDataPreviewResponse preview(Long flowId, String nodeId, NodeDataPreviewRequest request) {
        int limit = clampLimit(request == null ? null : request.getLimit());
        FlowEntity flow = flowService.getEntity(flowId);
        JsonNode dagJson = request != null && request.getDagJson() != null
                && !request.getDagJson().isNull()
                ? request.getDagJson() : flowService.parseDag(flow);
        if (dagJson == null) {
            throw new BizException(ErrorCode.DAG_INVALID, "画布尚未保存");
        }
        Dag dag = Dag.parse(dagJson);
        DagNode node = dag.node(nodeId);
        if (node == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "节点不存在：" + nodeId);
        }
        if ("source".equals(node.type())) {
            return previewSource(node, limit);
        }
        if ("sink".equals(node.type())) {
            return previewSink(flowId, dag, node, limit);
        }
        return previewDoris(flowId, node, limit);
    }

    /** Sink 数据预览：等价预览其唯一上游，附写入元信息；绝不触发写入。 */
    private NodeDataPreviewResponse previewSink(long flowId, Dag dag, DagNode node, int limit) {
        DagNode upstream = dag.firstUpstream(node.id());
        if (upstream == null) {
            throw new BizException(ErrorCode.DAG_INVALID, "Sink 节点没有上游节点：" + node.id(),
                    Collections.singletonMap("nodeId", node.id()));
        }
        NodeDataPreviewResponse response = "source".equals(upstream.type())
                ? previewSource(upstream, limit)
                : previewDoris(flowId, upstream, limit);
        response.setSink(sinkMeta(node));
        return response;
    }

    private Map<String, Object> sinkMeta(DagNode node) {
        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("targetType", node.text("targetType", "DORIS").trim().toUpperCase());
        sink.put("targetTable", node.text("tableName", ""));
        sink.put("writeMode", node.text("writeMode", "APPEND").trim().toUpperCase());
        JsonNode fields = node.array("fieldMapping");
        if (fields == null) {
            fields = node.array("fields");
        }
        List<Map<String, Object>> mapping = new ArrayList<>();
        if (fields != null) {
            for (JsonNode field : fields) {
                String source = field.path("source").asText(field.path("field").asText(""));
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("source", source);
                item.put("target", field.path("target").asText(source));
                item.put("type", field.path("type").asText(null));
                mapping.add(item);
            }
        }
        sink.put("fieldMapping", mapping);
        return sink;
    }

    private NodeDataPreviewResponse previewSource(DagNode node, int limit) {
        Long dsId = node.longValue("dataSourceId");
        String table = node.text("tableName", null);
        if (dsId == null || table == null || table.trim().isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR,
                    "Source 节点未选择数据源或数据表：" + node.id(),
                    Collections.singletonMap("nodeId", node.id()));
        }
        DataSourceEntity ds = dataSourceService.getEntity(dsId);
        String password = aesGcmCipher.decrypt(ds.getPasswordEnc());
        String safeTable = jdbcConnector.validateTableName(table.trim());

        List<String> fields = node.selectedFieldNames();
        List<Condition> filters = node.sourceFilters();
        Identifiers.QuoteStyle style = SourceSqlDialect.quoteStyle(ds.getType());
        ConditionCompiler.Compiled where = ConditionCompiler.compileSource(filters, null, style, true);
        ConditionCompiler.Compiled displayWhere = ConditionCompiler.compileSource(filters, null, style, false);

        StringBuilder select = new StringBuilder("SELECT ");
        if (fields.isEmpty()) {
            select.append('*');
        } else {
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    select.append(", ");
                }
                select.append(SourceSqlDialect.columnRef(ds.getType(), fields.get(i)));
            }
        }
        String from = " FROM " + SourceSqlDialect.tableRef(ds.getType(), ds.getDefaultDb(), safeTable);
        String whereSuffix = where.getExpr().isEmpty() ? "" : " WHERE " + where.getExpr();
        String limitSuffix = " LIMIT " + limit;
        String sql = select + from + whereSuffix + limitSuffix;
        String displaySql = select + from
                + (displayWhere.getExpr().isEmpty() ? "" : " WHERE " + displayWhere.getExpr())
                + limitSuffix;

        NodeDataPreviewResponse response = new NodeDataPreviewResponse();
        response.setSource("SOURCE_DB");
        response.setSql(sql);
        response.setDisplaySql(displaySql);
        response.setParams(where.getParams());
        querySource(ds, password, sql, where.getParams(), limit, response);
        return response;
    }

    private void querySource(DataSourceEntity ds, String password, String sql, List<Object> params,
                             int limit, NodeDataPreviewResponse response) {
        int timeout = Math.max(1, appProperties.getDataSource().getPreviewQueryTimeoutSeconds());
        try (Connection connection = jdbcConnector.openConnection(ds, password, timeout);
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setQueryTimeout(timeout);
            SourceSqlDialect.bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                fillRows(rs, limit, response);
            }
        } catch (BizException e) {
            throw e;
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "预览查询失败：" + e.getMessage());
        } catch (Exception e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "预览查询失败：" + e.getMessage());
        }
    }

    private NodeDataPreviewResponse previewDoris(Long flowId, DagNode node, int limit) {
        String table = nameSanitizer.middleTable(flowId, node.id());
        if (!dorisClient.tableExists(table)) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("nodeId", node.id());
            data.put("dorisTable", table);
            throw new BizException(ErrorCode.NODE_NOT_MATERIALIZED,
                    "节点尚未物化，请先执行任务", data);
        }
        List<ColumnInfo> columns = describeDorisTable(table);
        List<String> selected = new ArrayList<>();
        for (ColumnInfo c : columns) {
            if (!HIDDEN_COLUMN.equalsIgnoreCase(c.getName())) {
                selected.add(Identifiers.quote(c.getName(), Identifiers.QuoteStyle.BACKTICK));
            }
        }
        String projection = selected.isEmpty()
                ? "*" : String.join(", ", selected);
        String sql = "SELECT " + projection + " FROM " + dorisClient.qualifiedQuoted(table)
                + " LIMIT " + limit;

        NodeDataPreviewResponse response = new NodeDataPreviewResponse();
        response.setSource("DORIS");
        response.setSql(sql);
        response.setDisplaySql(sql);
        response.setParams(new ArrayList<>());
        queryDoris(sql, limit, columns.isEmpty(), response);
        return response;
    }

    private List<ColumnInfo> describeDorisTable(String table) {
        int timeout = Math.max(1, appProperties.getDataSource().getPreviewQueryTimeoutSeconds());
        List<ColumnInfo> columns = new ArrayList<>();
        String sql = "SHOW COLUMNS FROM " + dorisClient.qualifiedQuoted(table);
        try (Connection connection = dorisClient.open();
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(timeout);
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    columns.add(new ColumnInfo(rs.getString(1), rs.getString(2)));
                }
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED, "读取物化表结构失败：" + e.getMessage());
        }
        return columns;
    }

    private void queryDoris(String sql, int limit, boolean deriveColumns, NodeDataPreviewResponse response) {
        int timeout = Math.max(1, appProperties.getDataSource().getPreviewQueryTimeoutSeconds());
        try (Connection connection = dorisClient.open();
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(timeout);
            try (ResultSet rs = statement.executeQuery(sql)) {
                if (deriveColumns) {
                    response.setColumns(metaColumns(rs.getMetaData()));
                }
                fillRows(rs, limit, response);
            }
        } catch (BizException e) {
            throw e;
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED, "预览查询失败：" + e.getMessage());
        }
    }

    private void fillRows(ResultSet rs, int limit, NodeDataPreviewResponse response) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        List<ColumnInfo> columns = response.getColumns().isEmpty() ? metaColumns(meta) : response.getColumns();
        List<ColumnInfo> visible = new ArrayList<>();
        List<Integer> visibleIndexes = new ArrayList<>();
        for (ColumnInfo c : columns) {
            if (HIDDEN_COLUMN.equalsIgnoreCase(c.getName())) {
                continue;
            }
            int idx = findColumn(meta, c.getName());
            if (idx > 0) {
                visible.add(c);
                visibleIndexes.add(idx);
            }
        }
        response.setColumns(visible);
        List<List<Object>> rows = new ArrayList<>();
        while (rows.size() < limit && rs.next()) {
            List<Object> row = new ArrayList<>(visibleIndexes.size());
            for (Integer idx : visibleIndexes) {
                row.add(cell(rs, idx));
            }
            rows.add(row);
        }
        response.setRows(rows);
        response.setTruncated(rows.size() >= limit);
    }

    private static List<ColumnInfo> metaColumns(ResultSetMetaData meta) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            columns.add(new ColumnInfo(meta.getColumnLabel(i), meta.getColumnTypeName(i)));
        }
        return columns;
    }

    private static int findColumn(ResultSetMetaData meta, String name) throws SQLException {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (name.equalsIgnoreCase(meta.getColumnLabel(i))) {
                return i;
            }
        }
        return -1;
    }

    private static Object cell(ResultSet rs, int index) throws SQLException {
        Object value = rs.getObject(index);
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof byte[]) {
            return truncate(hex((byte[]) value));
        }
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            return truncate(clob.getSubString(1, (int) Math.min(clob.length(), CELL_MAX_BYTES * 4L)));
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toLocalDateTime().toString().replace('T', ' ');
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate().toString();
        }
        if (value instanceof Time) {
            return ((Time) value).toLocalTime().toString();
        }
        if (value instanceof LocalDateTime || value instanceof LocalDate || value instanceof LocalTime) {
            return value.toString().replace('T', ' ');
        }
        return truncate(String.valueOf(value));
    }

    private static String truncate(String value) {
        if (value == null) {
            return null;
        }
        if (value.getBytes(StandardCharsets.UTF_8).length <= CELL_MAX_BYTES) {
            return value;
        }
        int budget = CELL_MAX_BYTES - TRUNCATED_MARK.getBytes(StandardCharsets.UTF_8).length;
        int lo = 0;
        int hi = value.length();
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (value.substring(0, mid).getBytes(StandardCharsets.UTF_8).length <= budget) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return value.substring(0, lo) + TRUNCATED_MARK;
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static int clampLimit(Integer limit) {
        if (limit == null || limit < 1) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
}
