package com.datamodel.service.doris;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.config.DorisProperties;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.domain.SyncLogEntity;
import com.datamodel.domain.TableRegistryEntity;
import com.datamodel.dto.ColumnInfo;
import com.datamodel.service.connector.JdbcConnector;
import com.datamodel.sql.Condition;
import com.datamodel.sql.ConditionCompiler;
import com.datamodel.sql.Identifiers;
import com.datamodel.sql.SourceSqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 数据源副本同步：JDBC 分批读源库 → JSON Stream Load 写入 Doris。
 * 设计依据《数据同步与物理化设计-v1》§1/§2/§4。
 */
@Service
public class SourceSyncService {

    private static final Logger log = LoggerFactory.getLogger(SourceSyncService.class);
    private static final int FETCH_SIZE = 1000;

    private final DorisProperties props;
    private final DorisClient dorisClient;
    private final JdbcConnector jdbcConnector;
    private final TypeMappingRegistry typeMappingRegistry;
    private final DorisNameSanitizer nameSanitizer;
    private final DorisRegistryRepository registryRepository;
    private final SourceTableNameService sourceTableNameService;

    public SourceSyncService(DorisProperties props,
                             DorisClient dorisClient,
                             JdbcConnector jdbcConnector,
                             TypeMappingRegistry typeMappingRegistry,
                             DorisNameSanitizer nameSanitizer,
                             DorisRegistryRepository registryRepository,
                             SourceTableNameService sourceTableNameService) {
        this.props = props;
        this.dorisClient = dorisClient;
        this.jdbcConnector = jdbcConnector;
        this.typeMappingRegistry = typeMappingRegistry;
        this.nameSanitizer = nameSanitizer;
        this.registryRepository = registryRepository;
        this.sourceTableNameService = sourceTableNameService;
    }

    /**
     * @param selectedColumns 为空表示全表列
     * @param filters         Source 过滤条件，下推为源库 WHERE
     */
    public SyncResult sync(DataSourceEntity ds, String plainPassword, String sourceTable,
                           List<String> selectedColumns, List<Condition> filters,
                           long flowId, long runId, String nodeId) {
        dorisClient.requireConfigured();
        String table = jdbcConnector.validateTableName(sourceTable);
        List<ColumnInfo> allColumns = readColumns(ds, plainPassword, table);
        if (allColumns.isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "源表没有可同步的字段：" + table);
        }

        List<ColumnInfo> chosen = chooseColumns(allColumns, selectedColumns);
        List<TargetColumn> targets = mapColumns(ds.getType(), chosen);
        String fingerprint = fingerprint(targets);
        String dorisTable = sourceTableNameService.resolve(flowId, ds.getId(), table);

        boolean recreated = prepareTarget(dorisTable, fingerprint, targets);

        long sourceRows = countSource(ds, plainPassword, table, filters);
        long loaded = streamRows(ds, plainPassword, table, chosen, targets, filters,
                flowId, runId, nodeId, dorisTable);

        long targetRows = dorisClient.tableRowCount(dorisTable);
        if (sourceRows != targetRows) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                    "同步行数不一致：源库 " + sourceRows + " 行，Doris " + targetRows + " 行");
        }

        TableRegistryEntity registry = new TableRegistryEntity();
        registry.setDorisTable(dorisTable);
        registry.setKind("SRC");
        registry.setFlowId(flowId);
        registry.setNodeId(nodeId);
        registry.setDsId(ds.getId());
        registry.setSrcTable(table);
        registry.setSchemaFingerprint(fingerprint);
        registry.setKeyCols("[\"__dm_rowid\"]");
        registry.setRowCount(targetRows);
        registry.setByteSize(0L);
        registry.setStatus("READY");
        registryRepository.upsert(registry);

        SyncResult result = new SyncResult();
        result.setTargetTable(dorisTable);
        result.setSourceRows(sourceRows);
        result.setLoadedRows(loaded);
        result.setFingerprint(fingerprint);
        result.setRecreated(recreated);
        log.info("源表同步完成 ds={} table={} -> {} 行数={} 重建={}",
                ds.getId(), table, dorisTable, targetRows, recreated);
        return result;
    }

    private List<ColumnInfo> readColumns(DataSourceEntity ds, String plainPassword, String table) {
        try (Connection connection = jdbcConnector.openConnection(ds, plainPassword)) {
            return jdbcConnector.listColumns(connection, ds, table);
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "读取源表字段失败：" + e.getMessage());
        }
    }

    /**
     * 按 selected 的顺序输出列：用户在 Source 节点拖动字段后，Doris 副本表的列顺序
     * 也必须跟随该顺序，因此不能按源库物理列顺序遍历。
     */
    private List<ColumnInfo> chooseColumns(List<ColumnInfo> all, List<String> selected) {
        if (selected == null || selected.isEmpty()) {
            return all;
        }
        Map<String, ColumnInfo> byName = new HashMap<>();
        for (ColumnInfo c : all) {
            byName.putIfAbsent(c.getName(), c);
        }
        Set<String> wanted = new LinkedHashSet<>(selected);
        List<ColumnInfo> result = new ArrayList<>();
        Set<String> found = new LinkedHashSet<>();
        for (String name : wanted) {
            ColumnInfo c = byName.get(name);
            if (c == null) {
                continue;
            }
            result.add(c);
            found.add(name);
        }
        if (result.size() != wanted.size()) {
            List<String> missing = new ArrayList<>(wanted);
            missing.removeAll(found);
            throw new BizException(ErrorCode.PARAM_ERROR, "源表不存在这些字段：" + missing);
        }
        return result;
    }

    private List<TargetColumn> mapColumns(String dialect, List<ColumnInfo> columns) {
        List<TargetColumn> result = new ArrayList<>();
        Set<String> used = new LinkedHashSet<>();
        int index = 0;
        for (ColumnInfo c : columns) {
            String target = nameSanitizer.columnName(c.getName(), index);
            if ("__dm_rowid".equalsIgnoreCase(target)) {
                target = "__dm_rowid_2";
            }
            String unique = target;
            int suffix = 2;
            while (used.contains(unique.toLowerCase(Locale.ROOT))) {
                unique = target + "_" + suffix++;
            }
            used.add(unique.toLowerCase(Locale.ROOT));
            TypeMapping mapping = typeMappingRegistry.map(dialect, c.getType());
            TargetColumn tc = new TargetColumn(c.getName(), unique, mapping.getDorisType(), mapping.getWarning());
            result.add(tc);
            index++;
        }
        return result;
    }

    /**
     * 源表副本一律「先清理再重建」：information_schema 可能与物理元数据漂移
     * （瞬时查询失败被吞成不存在、DROP 后可见性延迟），若先 CREATE 会撞「src 表已存在」。
     * DROP TABLE IF EXISTS 同时清理残留表；CREATE 撞已存在时由 DorisClient.createTable 再重试一次。
     *
     * @return 是否因表结构变化而重建（供日志提示，不影响是否物理重建）
     */
    boolean prepareTarget(String dorisTable, String fingerprint, List<TargetColumn> targets) {
        boolean changed = false;
        if (dorisClient.tableExists(dorisTable)) {
            TableRegistryEntity registry = registryRepository.find(dorisTable);
            changed = registry == null || !fingerprint.equals(registry.getSchemaFingerprint());
        }
        dorisClient.dropTable(dorisTable);
        dorisClient.createTable(buildCreateDdl(dorisTable, targets), dorisTable);
        return changed;
    }

    private String buildCreateDdl(String dorisTable, List<TargetColumn> targets) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(dorisClient.qualified(dorisTable)).append(" (\n")
                .append("  `__dm_rowid` BIGINT NOT NULL");
        for (TargetColumn c : targets) {
            ddl.append(",\n  ").append(Identifiers.quote(c.targetName, Identifiers.QuoteStyle.BACKTICK))
                    .append(" ").append(c.dorisType).append(" NULL");
        }
        ddl.append("\n) DUPLICATE KEY(`__dm_rowid`) DISTRIBUTED BY HASH(`__dm_rowid`) BUCKETS 1 ")
                .append("PROPERTIES (\"replication_num\" = \"1\")");
        return ddl.toString();
    }

    private long countSource(DataSourceEntity ds, String plainPassword, String table, List<Condition> filters) {
        ConditionCompiler.Compiled where = compileSourceWhere(ds.getType(), filters);
        String sql = "SELECT COUNT(*) FROM "
                + SourceSqlDialect.tableRef(ds.getType(), ds.getDefaultDb(), table)
                + (where.getExpr().isEmpty() ? "" : " WHERE " + where.getExpr());
        try (Connection connection = jdbcConnector.openConnection(ds, plainPassword);
             PreparedStatement ps = connection.prepareStatement(sql)) {
            SourceSqlDialect.bind(ps, where.getParams());
            ps.setQueryTimeout(props.getTimeoutSeconds());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "统计源表行数失败：" + e.getMessage());
        }
    }

    private long streamRows(DataSourceEntity ds, String plainPassword, String table,
                            List<ColumnInfo> chosen, List<TargetColumn> targets,
                            List<Condition> filters, long flowId, long runId,
                            String nodeId, String dorisTable) {
        ConditionCompiler.Compiled where = compileSourceWhere(ds.getType(), filters);
        StringBuilder select = new StringBuilder("SELECT ");
        for (int i = 0; i < chosen.size(); i++) {
            if (i > 0) {
                select.append(", ");
            }
            select.append(SourceSqlDialect.columnRef(ds.getType(), chosen.get(i).getName()));
        }
        select.append(" FROM ")
                .append(SourceSqlDialect.tableRef(ds.getType(), ds.getDefaultDb(), table));
        if (!where.getExpr().isEmpty()) {
            select.append(" WHERE ").append(where.getExpr());
        }

        List<String> jsonPaths = new ArrayList<>();
        jsonPaths.add("$.__dm_rowid");
        for (TargetColumn c : targets) {
            jsonPaths.add("$." + c.targetName);
        }

        long seq = 0;
        long loaded = 0;
        int batchNo = 0;
        StringBuilder batch = new StringBuilder();
        int rowsInBatch = 0;
        try (Connection connection = jdbcConnector.openConnection(ds, plainPassword);
             PreparedStatement ps = connection.prepareStatement(select.toString(),
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(FETCH_SIZE);
            ps.setQueryTimeout(props.getTimeoutSeconds());
            SourceSqlDialect.bind(ps, where.getParams());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    seq++;
                    batch.append('{');
                    batch.append("\"__dm_rowid\":").append(seq);
                    for (int i = 0; i < targets.size(); i++) {
                        TargetColumn tc = targets.get(i);
                        batch.append(',');
                        appendJsonString(batch, tc.targetName);
                        batch.append(':');
                        appendJsonValue(batch, rs, i + 1);
                    }
                    batch.append('}').append('\n');
                    rowsInBatch++;
                    if (rowsInBatch >= props.getBatchRows()
                            || batch.length() >= props.getBatchBytes() * 2) {
                        loaded += flushBatch(batch, rowsInBatch, dorisTable, jsonPaths,
                                flowId, runId, nodeId, batchNo++);
                        batch.setLength(0);
                        rowsInBatch = 0;
                    }
                }
            }
            if (rowsInBatch > 0) {
                loaded += flushBatch(batch, rowsInBatch, dorisTable, jsonPaths,
                        flowId, runId, nodeId, batchNo);
            }
            return loaded;
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "读取源表失败：" + e.getMessage());
        }
    }

    private long flushBatch(StringBuilder batch, int rows, String dorisTable, List<String> jsonPaths,
                            long flowId, long runId, String nodeId, int batchNo) {
        byte[] body = batch.toString().getBytes(StandardCharsets.UTF_8);
        String label = label(flowId, runId, nodeId, batchNo);
        int attempts = Math.max(1, props.getRetryTimes() + 1);
        Exception last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            long start = System.currentTimeMillis();
            try {
                StreamLoadResult result = dorisClient.streamLoad(
                        dorisTable, body, label, jsonPaths, props.getTimeoutSeconds());
                logSuccess(dorisTable, label, attempt, rows, System.currentTimeMillis() - start);
                return result.getLoadedRows() > 0 ? result.getLoadedRows() : rows;
            } catch (BizException e) {
                last = e;
                logFailure(dorisTable, label, attempt, rows, e.getMessage(),
                        System.currentTimeMillis() - start);
                if (attempt < attempts) {
                    sleep(attempt == 1 ? 2000L : 4000L);
                }
            }
        }
        throw last == null
                ? new BizException(ErrorCode.DORIS_EXEC_FAILED, "Stream Load 失败")
                : (last instanceof BizException ? (BizException) last
                : new BizException(ErrorCode.DORIS_EXEC_FAILED, last.getMessage()));
    }

    private void logSuccess(String table, String label, int attempt, long rows, long cost) {
        SyncLogEntity entity = new SyncLogEntity();
        entity.setDorisTable(table);
        entity.setLabel(label);
        entity.setAttempt(attempt);
        entity.setStatus("SUCCESS");
        entity.setRowCount(rows);
        entity.setCostMs(cost);
        entity.setCreateTime(LocalDateTime.now());
        registryRepository.logSync(entity);
    }

    private void logFailure(String table, String label, int attempt, long rows, String msg, long cost) {
        SyncLogEntity entity = new SyncLogEntity();
        entity.setDorisTable(table);
        entity.setLabel(label);
        entity.setAttempt(attempt);
        entity.setStatus("FAILED");
        entity.setRowCount(0L);
        entity.setErrMsg(msg == null ? null : (msg.length() > 1000 ? msg.substring(0, 1000) : msg));
        entity.setCostMs(cost);
        entity.setCreateTime(LocalDateTime.now());
        registryRepository.logSync(entity);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ConditionCompiler.Compiled compileSourceWhere(String dialect, List<Condition> filters) {
        return ConditionCompiler.compileSource(filters, null,
                SourceSqlDialect.quoteStyle(dialect), true);
    }

    private static void appendJsonString(StringBuilder sb, String value) {
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                    break;
            }
        }
        sb.append('"');
    }

    private static void appendJsonValue(StringBuilder sb, ResultSet rs, int index) throws Exception {
        int type = rs.getMetaData().getColumnType(index);
        Object value;
        switch (type) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                value = rs.getBigDecimal(index);
                if (value == null) {
                    sb.append("null");
                } else {
                    sb.append(((java.math.BigDecimal) value).toPlainString());
                }
                return;
            case Types.BOOLEAN:
            case Types.BIT:
                boolean b = rs.getBoolean(index);
                if (rs.wasNull()) {
                    sb.append("null");
                } else {
                    sb.append(b);
                }
                return;
            case Types.DATE:
                java.sql.Date date = rs.getDate(index);
                if (date == null) {
                    sb.append("null");
                } else {
                    appendJsonString(sb, date.toString());
                }
                return;
            case Types.TIME:
                java.sql.Time time = rs.getTime(index);
                if (time == null) {
                    sb.append("null");
                } else {
                    appendJsonString(sb, time.toString());
                }
                return;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                java.sql.Timestamp ts = rs.getTimestamp(index);
                if (ts == null) {
                    sb.append("null");
                } else {
                    appendJsonString(sb, ts.toString());
                }
                return;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                byte[] bytes = rs.getBytes(index);
                if (bytes == null) {
                    sb.append("null");
                } else {
                    appendJsonString(sb, hex(bytes));
                }
                return;
            case Types.CLOB:
            case Types.NCLOB:
                Clob clob = rs.getClob(index);
                if (clob == null) {
                    sb.append("null");
                } else {
                    appendJsonString(sb, clob.getSubString(1, (int) Math.min(clob.length(), Integer.MAX_VALUE)));
                }
                return;
            default:
                String text = rs.getString(index);
                if (text == null) {
                    sb.append("null");
                } else {
                    appendJsonString(sb, text);
                }
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static String label(long flowId, long runId, String nodeId, int batchNo) {
        String raw = "dm_" + flowId + "_" + runId + "_" + nodeId + "_" + batchNo;
        String cleaned = raw.replaceAll("[^A-Za-z0-9_]", "_");
        return cleaned.length() > 128 ? cleaned.substring(0, 128) : cleaned;
    }

    private static String fingerprint(List<TargetColumn> columns) {
        StringBuilder sb = new StringBuilder();
        for (TargetColumn c : columns) {
            sb.append(c.targetName).append(':').append(c.dorisType).append(';');
        }
        return sha256(sb.toString()).substring(0, 16);
    }

    private static String sha256(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static final class TargetColumn {
        final String sourceName;
        final String targetName;
        final String dorisType;
        final String warning;

        TargetColumn(String sourceName, String targetName, String dorisType, String warning) {
            this.sourceName = sourceName;
            this.targetName = targetName;
            this.dorisType = dorisType;
            this.warning = warning;
        }
    }
}
