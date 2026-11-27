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
import com.datamodel.service.doris.SourceTableNameService;
import com.datamodel.service.llm.LlmExecuteService;
import com.datamodel.sql.Condition;
import com.datamodel.sql.ConditionCompiler;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.Identifiers;
import com.datamodel.sql.SourceSqlDialect;
import com.datamodel.sql.SqlBuildResult;
import com.datamodel.sql.SqlBuilderFactory;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private final SourceTableNameService sourceTableNameService;
    private final SqlBuilderFactory sqlBuilderFactory;
    private final LlmExecuteService llmExecuteService;

    public NodeDataService(FlowService flowService,
                           DataSourceService dataSourceService,
                           AesGcmCipher aesGcmCipher,
                           JdbcConnector jdbcConnector,
                           AppProperties appProperties,
                           DorisClient dorisClient,
                           DorisNameSanitizer nameSanitizer,
                           SourceTableNameService sourceTableNameService,
                           SqlBuilderFactory sqlBuilderFactory,
                           LlmExecuteService llmExecuteService) {
        this.flowService = flowService;
        this.dataSourceService = dataSourceService;
        this.aesGcmCipher = aesGcmCipher;
        this.jdbcConnector = jdbcConnector;
        this.appProperties = appProperties;
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
        this.sourceTableNameService = sourceTableNameService;
        this.sqlBuilderFactory = sqlBuilderFactory;
        this.llmExecuteService = llmExecuteService;
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
            return previewSink(dagJson, flowId, dag, node, limit);
        }
        if ("llm".equals(node.type())) {
            return previewLlm(dagJson, flowId, dag, node, limit);
        }
        return previewDoris(dagJson, flowId, dag, node, limit);
    }

    /** 大模型预览：真实调用模型（≤20 行），只读、不建表、不写映射表。 */
    private NodeDataPreviewResponse previewLlm(JsonNode dagJson, long flowId, Dag dag,
                                               DagNode node, int limit) {
        ensureUpstreamMaterialized(flowId, dag, node);
        int maxRows = Math.max(1, appProperties.getLlm().getPreviewMaxRows());
        int effective = Math.min(limit, maxRows);
        LlmExecuteService.PreviewResult preview = llmExecuteService.preview(dagJson, flowId, node,
                effective);
        NodeDataPreviewResponse response = new NodeDataPreviewResponse();
        response.setSource("LLM");
        List<ColumnInfo> columns = new ArrayList<>();
        for (com.datamodel.sql.OutputColumn column : preview.columns()) {
            columns.add(new ColumnInfo(column.getName(), column.getDorisType()));
        }
        response.setColumns(columns);
        response.setRows(preview.rows());
        response.setTruncated(preview.rows().size() >= effective);
        String note = "-- 大模型节点：预览已真实调用模型（" + preview.rows().size()
                + " 行，≤" + maxRows + " 行），不建表、不物化、不写库";
        response.setSql(note);
        response.setDisplaySql(note);
        Map<String, Object> llm = new LinkedHashMap<>();
        llm.put("calledModel", true);
        llm.put("calls", preview.calls());
        llm.put("batches", preview.batches());
        response.setLlm(llm);
        return response;
    }

    /** Sink 数据预览：等价预览其唯一上游，附写入元信息；绝不触发写入。 */
    private NodeDataPreviewResponse previewSink(JsonNode dagJson, long flowId, Dag dag,
                                                DagNode node, int limit) {
        DagNode upstream = dag.firstUpstream(node.id());
        if (upstream == null) {
            throw new BizException(ErrorCode.DAG_INVALID, "Sink 节点没有上游节点：" + node.id(),
                    Collections.singletonMap("nodeId", node.id()));
        }
        NodeDataPreviewResponse response;
        if ("source".equals(upstream.type())) {
            response = previewSource(upstream, limit);
        } else if ("llm".equals(upstream.type())) {
            response = previewLlm(dagJson, flowId, dag, upstream, limit);
        } else {
            response = previewDoris(dagJson, flowId, dag, upstream, limit);
        }
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
        querySource(node.id(), ds, password, sql, where.getParams(), limit, response);
        return response;
    }

    private void querySource(String nodeId, DataSourceEntity ds, String password, String sql,
                             List<Object> params, int limit, NodeDataPreviewResponse response) {
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
            throw previewQueryFailure(nodeId, e);
        } catch (Exception e) {
            throw previewQueryFailure(nodeId, e);
        }
    }

    /**
     * 非 Source/Sink 预览：用当前草稿 dagJson 生成该节点 SQL，在 Doris 上直接取数。
     * 不物化、不建表、不写运行状态；上游中间表缺失时明确报错。
     */
    private NodeDataPreviewResponse previewDoris(JsonNode dagJson, long flowId, Dag dag,
                                                 DagNode node, int limit) {
        ensureUpstreamMaterialized(flowId, dag, node);
        SqlBuildResult built;
        try {
            built = sqlBuilderFactory.build(dagJson, flowId, node.id());
        } catch (BizException e) {
            throw e;
        } catch (RuntimeException e) {
            throw previewBuildFailure(flowId, node.id(), e);
        }
        if (!built.getUpstreamMissing().isEmpty()) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("nodeId", node.id());
            data.put("reason", "UPSTREAM_CONFIG_MISSING");
            data.put("missing", built.getUpstreamMissing());
            throw new BizException(ErrorCode.DAG_INVALID,
                    "节点上游配置不完整，请先检查上游节点：" + node.id(), data);
        }
        String sql = limitWrapperSql(built.getSql(), limit);

        NodeDataPreviewResponse response = new NodeDataPreviewResponse();
        response.setSource("DORIS");
        response.setSql(sql);
        response.setDisplaySql(sql);
        response.setParams(new ArrayList<>(built.getParams()));
        queryDoris(node.id(), sql, built.getParams(), limit, response);
        return response;
    }

    /** 预览 SQL 一律下推 LIMIT，外层只读包装，不改变内层语义。 */
    static String limitWrapperSql(String inner, int limit) {
        return "SELECT * FROM (\n" + inner + "\n) AS `dm_preview` LIMIT " + limit;
    }

    /** 预览前确认上游表已存在（源副本表或上游节点中间表），缺失时给出可操作的错误。 */
    private void ensureUpstreamMaterialized(long flowId, Dag dag, DagNode node) {
        for (DagNode ancestor : dag.ancestors(node.id())) {
            String table;
            if ("source".equals(ancestor.type())) {
                Long dsId = ancestor.longValue("dataSourceId");
                String name = ancestor.text("tableName", null);
                if (dsId == null || name == null || name.trim().isEmpty()) {
                    continue;
                }
                table = sourceTableNameService.resolve(flowId, dsId, name.trim());
            } else if ("sink".equals(ancestor.type())) {
                continue;
            } else {
                table = nameSanitizer.middleTable(flowId, ancestor.id());
            }
            if (!dorisClient.tableExists(table)) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("nodeId", ancestor.id());
                data.put("reason", "UPSTREAM_NOT_MATERIALIZED");
                data.put("dorisTable", table);
                throw new BizException(ErrorCode.NODE_NOT_MATERIALIZED,
                        "上游尚未物化，请先执行上游节点", data);
            }
        }
    }

    private void queryDoris(String nodeId, String sql, List<Object> params, int limit,
                            NodeDataPreviewResponse response) {
        int timeout = Math.max(1, appProperties.getDataSource().getPreviewQueryTimeoutSeconds());
        try (Connection connection = dorisClient.open();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setQueryTimeout(timeout);
            SourceSqlDialect.bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                response.setColumns(metaColumns(rs.getMetaData()));
                fillRows(rs, limit, response);
            }
        } catch (BizException e) {
            throw e;
        } catch (SQLException e) {
            throw previewQueryFailure(nodeId, e);
        } catch (Exception e) {
            throw previewQueryFailure(nodeId, e);
        }
    }

    private static final Pattern UNKNOWN_COLUMN =
            Pattern.compile("Unknown column '([^']+)'", Pattern.CASE_INSENSITIVE);

    /** 预览执行错误只回中文可操作提示；Doris/驱动英文原文只写服务端日志。 */
    private BizException previewQueryFailure(String nodeId, Exception e) {
        log.error("节点预览查询失败 node={}：{}", nodeId, e.getMessage(), e);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", nodeId);
        String column = unknownColumn(e.getMessage());
        if (column != null) {
            data.put("reason", "PREVIEW_UNKNOWN_COLUMN");
            data.put("column", column);
            return new BizException(ErrorCode.PARAM_ERROR,
                    "预览失败：字段「" + column + "」不存在，请检查该节点的字段配置（节点 " + nodeId + "）",
                    data);
        }
        data.put("reason", "PREVIEW_QUERY_FAILED");
        return new BizException(ErrorCode.PARAM_ERROR,
                "预览失败：查询执行失败，请检查该节点的配置后重试（节点 " + nodeId + "）", data);
    }

    /** 预览 SQL 生成失败（非参数类错误）：同样收敛为中文业务错误，不透出底层异常原文。 */
    private BizException previewBuildFailure(long flowId, String nodeId, RuntimeException e) {
        log.error("生成节点预览 SQL 失败 flow={} node={}", flowId, nodeId, e);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", nodeId);
        data.put("reason", "PREVIEW_SQL_BUILD_FAILED");
        return new BizException(ErrorCode.PARAM_ERROR,
                "预览失败：节点配置无法生成 SQL，请检查该节点的配置（节点 " + nodeId + "）", data);
    }

    private static String unknownColumn(String message) {
        if (message == null) {
            return null;
        }
        Matcher matcher = UNKNOWN_COLUMN.matcher(message);
        return matcher.find() ? matcher.group(1).trim() : null;
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
