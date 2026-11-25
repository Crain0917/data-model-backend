package com.datamodel.sink;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.crypto.AesGcmCipher;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.domain.SinkWatermarkEntity;
import com.datamodel.dto.ColumnInfo;
import com.datamodel.service.DataSourceService;
import com.datamodel.service.connector.JdbcConnector;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.datamodel.service.doris.SinkWatermarkRepository;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.OutputColumn;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Sink 执行：流式读上游物化表 → 目标写入 → 每批推进水位线（架构 M3.7 §18/§19/§20）。
 */
@Service
public class SinkWriteService {

    private static final Logger log = LoggerFactory.getLogger(SinkWriteService.class);
    private static final int MAX_ATTEMPTS = 3;
    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");

    private final DorisClient dorisClient;
    private final DorisNameSanitizer nameSanitizer;
    private final com.datamodel.service.doris.SourceTableNameService sourceTableNameService;
    private final SinkConfigParser configParser;
    private final SinkWatermarkRepository watermarkRepository;
    private final JdbcConnector jdbcConnector;
    private final DataSourceService dataSourceService;
    private final AesGcmCipher aesGcmCipher;
    private final ObjectMapper objectMapper;

    public SinkWriteService(DorisClient dorisClient,
                            DorisNameSanitizer nameSanitizer,
                            com.datamodel.service.doris.SourceTableNameService sourceTableNameService,
                            SinkConfigParser configParser,
                            SinkWatermarkRepository watermarkRepository,
                            JdbcConnector jdbcConnector,
                            DataSourceService dataSourceService,
                            AesGcmCipher aesGcmCipher,
                            ObjectMapper objectMapper) {
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
        this.sourceTableNameService = sourceTableNameService;
        this.configParser = configParser;
        this.watermarkRepository = watermarkRepository;
        this.jdbcConnector = jdbcConnector;
        this.dataSourceService = dataSourceService;
        this.aesGcmCipher = aesGcmCipher;
        this.objectMapper = objectMapper;
    }

    public interface LogSink {
        void log(String level, String message);
    }

    public SinkWriteResult write(long flowId, long runId, JsonNode dagJson, DagNode node,
                                 boolean reuseUpstream, LogSink logSink, SinkProgress progress) {
        SinkConfig config = configParser.parse(node);
        DagNode upstream = upstreamNode(dagJson, node);
        String upstreamTable = upstreamTable(upstream, flowId);
        List<ColumnInfo> physical = materializedColumns(node, upstream, upstreamTable);
        List<OutputColumn> outputColumns = new ArrayList<>();
        Set<String> physicalNames = new LinkedHashSet<>();
        for (ColumnInfo column : physical) {
            physicalNames.add(column.getName());
            if (!SinkSqlPlanner.isHidden(column.getName())) {
                outputColumns.add(new OutputColumn(column.getName(), column.getType()));
            }
        }
        config = configParser.resolve(config, outputColumns);
        configParser.validateAgainstPhysicalColumns(config, physicalNames);

        String watermarkType = typeOf(physical, config.getWatermarkColumn());
        if (!reuseUpstream || config.isOverwrite()) {
            watermarkRepository.deleteNodeTarget(flowId, node.id(), config.targetKey());
        }

        SinkWriteResult result = new SinkWriteResult();
        result.setTargetType(config.getTargetType());
        result.setTargetTable(config.getTableName());
        result.setWriteMode(config.getWriteMode());
        fillParams(result, config, upstreamTable);

        progress.setTargetType(config.getTargetType());
        progress.setTargetTable(config.getTableName());
        progress.setWriteMode(config.getWriteMode());
        progress.setWatermarkColumn(config.getWatermarkColumn());
        progress.getParams().putAll(result.getParams());

        if (config.isDoris()) {
            writeDoris(flowId, runId, node.id(), config, upstreamTable, watermarkType, logSink,
                    result, progress);
        } else {
            writeJdbc(flowId, runId, node.id(), config, upstreamTable, watermarkType, logSink,
                    result, progress);
        }
        return result;
    }

    /**
     * 按指定已物化源表写目标：行去重的重复行流复用完整的目标检查/建表/批次/重试逻辑。
     * 调用方负责把重复行物化成 Doris 中间表并传来输出列。
     */
    public SinkWriteResult writeTable(long flowId, long runId, String nodeId, String upstreamTable,
                                      List<OutputColumn> outputColumns, SinkConfig config,
                                      LogSink logSink, SinkProgress progress) {
        List<ColumnInfo> physical = materializedColumns(nodeId, upstreamTable);
        Set<String> physicalNames = new LinkedHashSet<>();
        for (ColumnInfo column : physical) {
            if (!SinkSqlPlanner.isHidden(column.getName())) {
                physicalNames.add(column.getName());
            }
        }
        config = configParser.resolve(config, outputColumns);
        configParser.validateAgainstPhysicalColumns(config, physicalNames);

        String watermarkType = typeOf(physical, config.getWatermarkColumn());
        if (config.isOverwrite()) {
            watermarkRepository.deleteNodeTarget(flowId, nodeId, config.targetKey());
        }
        SinkWriteResult result = new SinkWriteResult();
        result.setTargetType(config.getTargetType());
        result.setTargetTable(config.getTableName());
        result.setWriteMode(config.getWriteMode());
        fillParams(result, config, upstreamTable);
        progress.setTargetType(config.getTargetType());
        progress.setTargetTable(config.getTableName());
        progress.setWriteMode(config.getWriteMode());
        progress.setWatermarkColumn(config.getWatermarkColumn());
        progress.getParams().putAll(result.getParams());

        if (config.isDoris()) {
            writeDoris(flowId, runId, nodeId, config, upstreamTable, watermarkType, logSink, result,
                    progress);
        } else {
            writeJdbc(flowId, runId, nodeId, config, upstreamTable, watermarkType, logSink, result,
                    progress);
        }
        return result;
    }

    /** 重复行中间表（已物化）的物理列检查，错误语义与普通上游一致。 */
    private List<ColumnInfo> materializedColumns(String nodeId, String table) {
        if (!dorisClient.tableExists(table)) {
            throw new BizException(ErrorCode.NODE_NOT_MATERIALIZED,
                    "重复行中间表尚未物化：" + table,
                    java.util.Collections.singletonMap("nodeId", nodeId));
        }
        List<ColumnInfo> physical = dorisClient.listColumns(table);
        if (physical.isEmpty()) {
            throw new BizException(ErrorCode.NODE_NOT_MATERIALIZED,
                    "重复行中间表尚未物化：" + table,
                    java.util.Collections.singletonMap("nodeId", nodeId));
        }
        return physical;
    }

    /** reuseUpstream 同步预检：只读已物化的上游表，不再重算祖先（D-M3.7-06）。 */
    public void ensureUpstreamMaterialized(long flowId, JsonNode dagJson, DagNode sinkNode) {
        DagNode upstream = upstreamNode(dagJson, sinkNode);
        String table = upstreamTable(upstream, flowId);
        materializedColumns(sinkNode, upstream, table);
    }

    private DagNode upstreamNode(JsonNode dagJson, DagNode node) {
        DagNode upstream = Dag.parse(dagJson).firstUpstream(node.id());
        if (upstream == null) {
            throw new BizException(ErrorCode.DAG_INVALID, "Sink 节点没有上游节点：" + node.id(),
                    java.util.Collections.singletonMap("nodeId", node.id()));
        }
        return upstream;
    }

    private List<ColumnInfo> materializedColumns(DagNode node, DagNode upstream, String table) {
        if (!dorisClient.tableExists(table)) {
            throw upstreamNotReady(node, upstream, table);
        }
        List<ColumnInfo> physical = dorisClient.listColumns(table);
        if (physical.isEmpty()) {
            throw upstreamNotReady(node, upstream, table);
        }
        return physical;
    }

    // ----------------------------------------------------------------- Doris

    private void writeDoris(long flowId, long runId, String nodeId,
                            SinkConfig config, String upstreamTable, String watermarkType,
                            LogSink logSink, SinkWriteResult result, SinkProgress progress) {
        DataSourceEntity ds = targetDataSource(config);
        String password;
        try {
            password = aesGcmCipher.decrypt(ds.getPasswordEnc());
        } catch (RuntimeException e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "Doris 目标数据源口令解密失败");
        }
        DorisClient.DorisEndpoint target = dorisClient.endpointFor(ds, password);
        config.setTargetLabelOverride(target.describe() + "." + config.getTableName());

        List<ColumnInfo> targetColumns;
        try {
            targetColumns = prepareDorisTarget(config, target, progress);
            if (config.isOverwrite()) {
                dorisClient.truncate(target, config.getTableName());
                logSink.log("INFO", "覆盖写入：已清空目标表 " + config.targetLabel());
            }
        } catch (BizException e) {
            throw dorisTargetFailure(target, "目标检查/建表", e);
        }

        SinkWatermarkEntity watermark = loadWatermark(flowId, nodeId, config);
        initWatermarkProgress(progress, watermark);
        boolean resume = canResume(config, watermark);
        SinkSqlPlanner.SinkReadPlan plan = SinkSqlPlanner.plan(upstreamTable, config);
        if (!resume) {
            plan = planWithoutWatermark(config, upstreamTable);
        }
        progress.setSqlText("Stream Load → " + config.targetLabel());
        writeDorisRows(flowId, runId, nodeId, config, target, plan, resume, watermark,
                watermarkType, targetColumns, logSink, result, progress);
        result.setSqlText("Stream Load → " + config.targetLabel());
    }

    /** 目标侧连接/权限类失败统一归为 5003 并指明数据源与阶段；配置类错误原样抛出。 */
    private BizException dorisTargetFailure(DorisClient.DorisEndpoint target, String stage,
                                            BizException cause) {
        if (cause.getErrorCode() == ErrorCode.DORIS_EXEC_FAILED
                || cause.getErrorCode() == ErrorCode.DRIVER_ERROR) {
            return new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：目标 " + target.describe() + " " + stage + "失败："
                            + safe(cause));
        }
        return cause;
    }

    private void writeDorisRows(long flowId, long runId, String nodeId, SinkConfig config,
                                DorisClient.DorisEndpoint target,
                                SinkSqlPlanner.SinkReadPlan plan, boolean resume,
                                SinkWatermarkEntity watermark, String watermarkType,
                                List<ColumnInfo> targetColumns, LogSink logSink,
                                SinkWriteResult result, SinkProgress progress) {
        List<String> targetColumnNames = new ArrayList<>();
        for (ColumnInfo column : targetColumns) {
            targetColumnNames.add(column.getName());
        }
        List<String> jsonPaths = dorisJsonPaths(config, targetColumnNames);
        String[] bodyKeys = jsonBodyKeys(config, targetColumnNames);
        boolean hasSeqColumn = hasSequenceColumn(targetColumns);
        int[] sourceIndexes = sourceIndexes(config, plan);
        int rowidIndex = plan.getReadColumns().indexOf(SinkSqlPlanner.ROWID_COLUMN);
        int watermarkIndex = watermarkIndex(config, plan);
        long committed = watermark == null || watermark.getCommittedRows() == null
                ? 0L : watermark.getCommittedRows();
        long rows = 0;
        int batches = 0;
        int batchNo = 0;
        List<Object[]> batch = new ArrayList<>();
        long seq = committed;
        String labelPrefix = labelPrefix(runId, nodeId);

        try (Connection connection = dorisClient.open();
             PreparedStatement ps = connection.prepareStatement(plan.getSql(),
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setFetchSize(Integer.MIN_VALUE);
            if (resume) {
                ps.setString(1, watermark.getLastValue());
                ps.setString(2, watermark.getLastValue());
                ps.setLong(3, watermark.getLastRowid() == null ? 0L : watermark.getLastRowid());
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Object[] row = new Object[plan.getReadColumns().size()];
                    for (int i = 0; i < row.length; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    batch.add(row);
                    if (batch.size() >= config.getBatchSize()) {
                        batchNo++;
                        Object[] last = batch.get(batch.size() - 1);
                        String label = labelPrefix + "_" + batchNo;
                        int written = writeDorisBatch(config, target, batch, sourceIndexes,
                                jsonPaths, bodyKeys, hasSeqColumn, seq, label, logSink);
                        rows += written;
                        committed += written;
                        seq += written;
                        batches++;
                        advanceWatermark(flowId, nodeId, config, watermark,
                                last, rowidIndex, watermarkIndex, watermarkType, committed, logSink,
                                progress);
                        progress.setRowsWritten(rows);
                        progress.setBatches(batches);
                        progress.setLastBatchNo(batchNo);
                        String message = "第 " + batchNo + " 批 Stream Load 完成：label=" + label
                                + "，目标=" + config.targetLabel() + "，写入 " + written
                                + " 行，累计 " + rows + " 行";
                        logSink.log("INFO", message);
                        log.info(message);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    batchNo++;
                    Object[] last = batch.get(batch.size() - 1);
                    String label = labelPrefix + "_" + batchNo;
                    int written = writeDorisBatch(config, target, batch, sourceIndexes,
                            jsonPaths, bodyKeys, hasSeqColumn, seq, label, logSink);
                    rows += written;
                    committed += written;
                    seq += written;
                    batches++;
                    advanceWatermark(flowId, nodeId, config, watermark,
                            last, rowidIndex, watermarkIndex, watermarkType, committed, logSink,
                            progress);
                    progress.setRowsWritten(rows);
                    progress.setBatches(batches);
                    progress.setLastBatchNo(batchNo);
                    String message = "第 " + batchNo + " 批 Stream Load 完成：label=" + label
                            + "，目标=" + config.targetLabel() + "，写入 " + written
                            + " 行，累计 " + rows + " 行";
                    logSink.log("INFO", message);
                    log.info(message);
                }
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：读取上游失败：" + safe(e));
        }
        result.setRows(rows);
        result.setBatches(batches);
    }

    private int writeDorisBatch(SinkConfig config, DorisClient.DorisEndpoint target,
                                List<Object[]> rows, int[] sourceIndexes, List<String> jsonPaths,
                                String[] bodyKeys, boolean hasSeqColumn, long seqBase,
                                String label, LogSink logSink) {
        byte[] body = buildJsonBody(config, rows, sourceIndexes, bodyKeys, hasSeqColumn, seqBase);
        RuntimeException loadDetail = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                dorisClient.streamLoad(target, config.getTableName(), body, label, jsonPaths, 1800);
                return rows.size();
            } catch (RuntimeException e) {
                String detailMessage = safe(e);
                if (detailMessage.contains("_load_error_log")
                        || detailMessage.contains("ErrorURL")) {
                    loadDetail = e;
                }
                if (attempt >= MAX_ATTEMPTS) {
                    throw sinkWriteFailed(config, label, loadDetail == null ? e : loadDetail);
                }
                String message = "Stream Load 第 " + attempt + " 次失败：label=" + label
                        + "，目标=" + config.targetLabel() + "，delayMs=" + backoff(attempt)
                        + "，原因=" + safe(e);
                logSink.log("WARN", message);
                log.warn(message);
                sleep(backoff(attempt));
            }
        }
        throw sinkWriteFailed(config, label, null);
    }

    /**
     * jsonpaths 必须严格按目标表物理列顺序；UNIQUE 模型键列在前会重排字段映射，
     * 若仍按映射顺序下发会造成 Stream Load 错列。
     */
    static List<String> dorisJsonPaths(SinkConfig config, List<String> physicalColumns) {
        Set<String> mapped = new HashSet<>();
        for (SinkField field : config.getFieldMapping()) {
            mapped.add(field.getTarget().toLowerCase(Locale.ROOT));
        }
        List<String> jsonPaths = new ArrayList<>();
        for (String column : physicalColumns) {
            if (SinkDdlBuilder.SEQUENCE_COLUMN.equalsIgnoreCase(column)
                    || mapped.contains(column.toLowerCase(Locale.ROOT))) {
                jsonPaths.add("$." + column);
            }
        }
        return jsonPaths;
    }

    /** JSON body 键与 jsonpaths 取同一物理列名，避免大小写差异导致取值为空。 */
    static String[] jsonBodyKeys(SinkConfig config, List<String> physicalColumns) {
        Map<String, String> byLower = new LinkedHashMap<>();
        for (String column : physicalColumns) {
            byLower.put(column.toLowerCase(Locale.ROOT), column);
        }
        String[] keys = new String[config.getFieldMapping().size()];
        for (int i = 0; i < keys.length; i++) {
            String target = config.getFieldMapping().get(i).getTarget();
            String actual = byLower.get(target.toLowerCase(Locale.ROOT));
            keys[i] = actual == null ? target : actual;
        }
        return keys;
    }

    private byte[] buildJsonBody(SinkConfig config, List<Object[]> rows, int[] sourceIndexes,
                                 String[] bodyKeys, boolean hasSeqColumn, long seqBase) {
        StringBuilder body = new StringBuilder();
        for (int r = 0; r < rows.size(); r++) {
            Map<String, Object> object = new LinkedHashMap<>();
            Object[] row = rows.get(r);
            for (int i = 0; i < config.getFieldMapping().size(); i++) {
                object.put(bodyKeys[i], jsonValue(row[sourceIndexes[i]]));
            }
            if (hasSeqColumn) {
                object.put(SinkDdlBuilder.SEQUENCE_COLUMN, seqBase + r + 1);
            }
            try {
                body.append(objectMapper.writeValueAsString(object));
            } catch (Exception e) {
                throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                        "Sink 写入失败：序列化批次数据失败：" + safe(e));
            }
            body.append('\n');
        }
        return body.toString().getBytes(StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ JDBC

    private void writeJdbc(long flowId, long runId, String nodeId,
                           SinkConfig config, String upstreamTable, String watermarkType,
                           LogSink logSink, SinkWriteResult result, SinkProgress progress) {
        DataSourceEntity ds = targetDataSource(config);
        String password;
        try {
            password = aesGcmCipher.decrypt(ds.getPasswordEnc());
        } catch (RuntimeException e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "目标数据源口令解密失败");
        }
        Connection target = null;
        try {
            target = jdbcConnector.openConnection(ds, password, null);
            List<ColumnInfo> actual = prepareJdbcTarget(target, ds, config, logSink, progress);
            rewriteTargetNames(config, actual);
            target.setAutoCommit(false);

            if (config.isOverwrite()) {
                try (Statement st = target.createStatement()) {
                    st.execute("TRUNCATE TABLE " + SinkDdlBuilder.qualified(
                            config.getTargetType(), config.getTableName()));
                }
                target.commit();
                logSink.log("INFO", "覆盖写入：已清空目标表 " + config.getTableName());
            }

            SinkWatermarkEntity watermark = loadWatermark(flowId, nodeId, config);
            initWatermarkProgress(progress, watermark);
            boolean resume = canResume(config, watermark);
            SinkSqlPlanner.SinkReadPlan plan = resume
                    ? SinkSqlPlanner.plan(upstreamTable, config)
                    : planWithoutWatermark(config, upstreamTable);
            String sqlText = "INSERT INTO " + config.getTableName() + "("
                    + config.getFieldMapping().size() + " cols) 批插 → " + config.targetLabel();
            progress.setSqlText(sqlText);
            writeJdbcRows(target, flowId, runId, nodeId, config, ds, plan, resume, watermark,
                    watermarkType, logSink, result, progress);
            result.setSqlText(sqlText);
        } catch (SQLException e) {
            throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：连接或读取目标库失败：" + safe(e));
        } catch (ClassNotFoundException e) {
            throw new BizException(ErrorCode.DRIVER_ERROR, "目标数据源驱动未加载：" + ds.getType());
        } finally {
            closeQuietly(target);
        }
    }

    private void writeJdbcRows(Connection target, long flowId, long runId, String nodeId,
                               SinkConfig config,
                               DataSourceEntity ds, SinkSqlPlanner.SinkReadPlan plan,
                               boolean resume, SinkWatermarkEntity watermark,
                               String watermarkType, LogSink logSink, SinkWriteResult result,
                               SinkProgress progress) {
        int[] sourceIndexes = sourceIndexes(config, plan);
        int rowidIndex = plan.getReadColumns().indexOf(SinkSqlPlanner.ROWID_COLUMN);
        int watermarkIndex = watermarkIndex(config, plan);
        long committed = watermark == null || watermark.getCommittedRows() == null
                ? 0L : watermark.getCommittedRows();
        long rows = 0;
        int batches = 0;
        int batchNo = 0;
        List<Object[]> batch = new ArrayList<>();

        String sql = insertSql(config);
        try (Connection connection = dorisClient.open();
             PreparedStatement source = connection.prepareStatement(plan.getSql(),
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             PreparedStatement insert = target.prepareStatement(sql)) {
            source.setFetchSize(Integer.MIN_VALUE);
            if (resume) {
                source.setString(1, watermark.getLastValue());
                source.setString(2, watermark.getLastValue());
                source.setLong(3, watermark.getLastRowid() == null ? 0L : watermark.getLastRowid());
            }
            try (ResultSet rs = source.executeQuery()) {
                while (rs.next()) {
                    Object[] row = new Object[plan.getReadColumns().size()];
                    for (int i = 0; i < row.length; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    batch.add(row);
                    if (batch.size() >= config.getBatchSize()) {
                        batchNo++;
                        Object[] last = batch.get(batch.size() - 1);
                        writeJdbcBatch(target, insert, config, batch, sourceIndexes, logSink, batchNo);
                        rows += batch.size();
                        committed += batch.size();
                        batches++;
                        advanceWatermark(flowId, nodeId, config, watermark,
                                last, rowidIndex, watermarkIndex, watermarkType, committed, logSink,
                                progress);
                        progress.setRowsWritten(rows);
                        progress.setBatches(batches);
                        progress.setLastBatchNo(batchNo);
                        String message = "第 " + batchNo + " 批 JDBC 提交完成：目标="
                                + config.targetLabel() + "，写入 " + batch.size()
                                + " 行，累计 " + rows + " 行";
                        logSink.log("INFO", message);
                        log.info(message);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    batchNo++;
                    Object[] last = batch.get(batch.size() - 1);
                    int size = batch.size();
                    writeJdbcBatch(target, insert, config, batch, sourceIndexes, logSink, batchNo);
                    rows += size;
                    committed += size;
                    batches++;
                    advanceWatermark(flowId, nodeId, config, watermark,
                            last, rowidIndex, watermarkIndex, watermarkType, committed, logSink,
                            progress);
                    progress.setRowsWritten(rows);
                    progress.setBatches(batches);
                    progress.setLastBatchNo(batchNo);
                    String message = "第 " + batchNo + " 批 JDBC 提交完成：目标="
                            + config.targetLabel() + "，写入 " + size
                            + " 行，累计 " + rows + " 行";
                    logSink.log("INFO", message);
                    log.info(message);
                }
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：读取上游失败：" + safe(e));
        }
        result.setRows(rows);
        result.setBatches(batches);
    }

    private void writeJdbcBatch(Connection target, PreparedStatement insert, SinkConfig config,
                                List<Object[]> rows, int[] sourceIndexes, LogSink logSink,
                                int batchNo) {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                insert.clearBatch();
                for (Object[] row : rows) {
                    for (int i = 0; i < sourceIndexes.length; i++) {
                        insert.setObject(i + 1, row[sourceIndexes[i]]);
                    }
                    insert.addBatch();
                }
                insert.executeBatch();
                target.commit();
                return;
            } catch (SQLException e) {
                rollbackQuietly(target);
                if (attempt >= MAX_ATTEMPTS) {
                    throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                            "Sink 写入失败：第 " + batchNo + " 批 JDBC 批插重试耗尽：" + safe(e));
                }
                String message = "JDBC 批插第 " + attempt + " 次失败：batchNo=" + batchNo
                        + "，目标=" + config.targetLabel() + "，delayMs=" + backoff(attempt)
                        + "，原因=" + safe(e);
                logSink.log("WARN", message);
                log.warn(message);
                sleep(backoff(attempt));
            }
        }
    }

    private List<ColumnInfo> prepareJdbcTarget(Connection target, DataSourceEntity ds,
                                               SinkConfig config, LogSink logSink,
                                               SinkProgress progress) {
        try {
            boolean exists = jdbcConnector.tableExists(target, ds, config.getTableName());
            if (!exists) {
                if (!config.isAutoCreateTable()) {
                    throw tableMissing(config);
                }
                createJdbcTable(target, ds, config, logSink);
                progress.setTableCreated(true);
                exists = jdbcConnector.tableExists(target, ds, config.getTableName());
                if (!exists) {
                    throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                            "Sink 写入失败：目标表创建后仍不存在：" + config.getTableName());
                }
            }
            List<ColumnInfo> actual = jdbcConnector.describeColumns(target, ds, config.getTableName());
            validateStructure(config, actual, false);
            return actual;
        } catch (SQLException e) {
            throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：目标表检查失败：" + safe(e));
        }
    }

    private void createJdbcTable(Connection target, DataSourceEntity ds, SinkConfig config,
                                 LogSink logSink) throws SQLException {
        String ddl = SinkDdlBuilder.buildCreateTable(config, SinkDdlBuilder.qualified(
                config.getTargetType(), config.getTableName()));
        try (Statement st = target.createStatement()) {
            st.execute(ddl);
            logSink.log("INFO", "目标表不存在，已自动建表：" + config.getTableName());
        } catch (SQLException e) {
            if (isDuplicateTable(e)) {
                logSink.log("WARN", "目标表已由并发创建，继续结构校验：" + config.getTableName());
                return;
            }
            throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：自动建表失败：" + safe(e));
        }
    }

    // --------------------------------------------------------------- Doris DDL

    private List<ColumnInfo> prepareDorisTarget(SinkConfig config, DorisClient.DorisEndpoint target,
                                                SinkProgress progress) {
        boolean exists = dorisClient.tableExistsStrict(target, config.getTableName());
        boolean existed = exists;
        if (!exists) {
            if (!config.isAutoCreateTable()) {
                throw tableMissing(config);
            }
            dorisClient.execute(target, SinkDdlBuilder.buildCreateTable(config,
                    dorisClient.qualifiedQuoted(target, config.getTableName())));
            progress.setTableCreated(true);
            exists = dorisClient.tableExistsStrict(target, config.getTableName());
            if (!exists) {
                throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                        "Sink 写入失败：目标表创建后仍不存在：" + config.getTableName());
            }
        }
        List<ColumnInfo> actual = dorisClient.listColumns(target, config.getTableName());
        validateStructure(config, actual, true);
        // 仅对本次运行前已存在、且显式配置了模型的表校验实际模型/主键。
        if (config.isTableModelExplicit() && existed) {
            validateTableModel(config, target);
        }
        return actual;
    }

    private void validateTableModel(SinkConfig config, DorisClient.DorisEndpoint target) {
        DorisClient.TableMeta meta = dorisClient.describeTableModel(target, config.getTableName());
        String expected = config.getTableModel();
        String actualModel = meta.getModel();
        if (actualModel == null || !expected.equalsIgnoreCase(actualModel)) {
            throw SinkConfigParser.invalid(config.getNodeId(), "TABLE_MODEL_MISMATCH",
                    "目标表建表模型为 " + (actualModel == null ? "未知" : actualModel)
                            + "，与配置的 " + expected + " 不一致");
        }
        if (!config.isUniqueModel()) {
            return;
        }
        Set<String> expectedKeys = new LinkedHashSet<>();
        for (String key : config.getKeyColumns()) {
            expectedKeys.add(key.toLowerCase(Locale.ROOT));
        }
        Set<String> actualKeys = new LinkedHashSet<>();
        for (String key : meta.getKeyColumns()) {
            actualKeys.add(key.toLowerCase(Locale.ROOT));
        }
        if (!expectedKeys.equals(actualKeys)) {
            throw SinkConfigParser.invalid(config.getNodeId(), "KEY_COLUMNS_MISMATCH",
                    "目标表主键与配置不一致：配置 " + config.getKeyColumns()
                            + "，实际 " + meta.getKeyColumns());
        }
    }

    private boolean hasSequenceColumn(List<ColumnInfo> columns) {
        for (ColumnInfo column : columns) {
            if (SinkDdlBuilder.SEQUENCE_COLUMN.equalsIgnoreCase(column.getName())) {
                return true;
            }
        }
        return false;
    }

    // ----------------------------------------------------------- validation

    private void validateStructure(SinkConfig config, List<ColumnInfo> actual, boolean doris) {
        Map<String, ColumnInfo> actualByName = new LinkedHashMap<>();
        List<Map<String, Object>> actualList = new ArrayList<>();
        for (ColumnInfo column : actual) {
            if (doris && SinkDdlBuilder.SEQUENCE_COLUMN.equalsIgnoreCase(column.getName())) {
                continue;
            }
            actualByName.put(column.getName().toLowerCase(Locale.ROOT), column);
            actualList.add(columnMap(column.getName(), column.getType()));
        }
        List<Map<String, Object>> expectedList = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        Set<String> expectedNames = new LinkedHashSet<>();
        for (SinkField field : config.getFieldMapping()) {
            String key = field.getTarget().toLowerCase(Locale.ROOT);
            expectedNames.add(key);
            String expectedType = TargetTypeMapper.toTarget(config.getTargetType(), field.getType());
            expectedList.add(columnMap(field.getTarget(), expectedType));
            ColumnInfo actualColumn = actualByName.get(key);
            if (actualColumn == null) {
                problems.add("缺少列 " + field.getTarget());
            } else if (!TargetTypeMapper.compatible(config.getTargetType(),
                    field.getType(), actualColumn.getType())) {
                problems.add("列 " + field.getTarget() + " 类型不一致：期望 " + expectedType
                        + "，实际 " + actualColumn.getType());
            }
        }
        for (String actualName : actualByName.keySet()) {
            if (!expectedNames.contains(actualName)) {
                problems.add("目标表存在多余列 " + actualByName.get(actualName).getName());
            }
        }
        if (!problems.isEmpty()) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("nodeId", config.getNodeId());
            data.put("targetType", config.getTargetType());
            data.put("tableName", config.getTableName());
            data.put("expected", expectedList);
            data.put("actual", actualList);
            data.put("problems", problems);
            throw new BizException(ErrorCode.SINK_TABLE_MISMATCH,
                    "目标表结构与上游字段不一致，请重建目标表或调整字段映射", data);
        }
    }

    private void rewriteTargetNames(SinkConfig config, List<ColumnInfo> actual) {
        Map<String, String> actualNames = new HashMap<>();
        for (ColumnInfo column : actual) {
            actualNames.put(column.getName().toLowerCase(Locale.ROOT), column.getName());
        }
        List<SinkField> rewritten = new ArrayList<>();
        for (SinkField field : config.getFieldMapping()) {
            String name = actualNames.get(field.getTarget().toLowerCase(Locale.ROOT));
            rewritten.add(new SinkField(field.getSource(),
                    name == null ? field.getTarget() : name, field.getType()));
        }
        config.setFieldMapping(rewritten);
    }

    // ------------------------------------------------------------- watermark

    private SinkWatermarkEntity loadWatermark(long flowId, String nodeId, SinkConfig config) {
        try {
            return watermarkRepository.find(flowId, nodeId, config.targetKey());
        } catch (RuntimeException e) {
            throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：读取水位线失败：" + safe(e));
        }
    }

    private boolean canResume(SinkConfig config, SinkWatermarkEntity watermark) {
        return config.getWatermarkColumn() != null
                && watermark != null
                && watermark.getLastValue() != null
                && !watermark.getLastValue().isEmpty()
                && watermark.getLastRowid() != null;
    }

    private SinkSqlPlanner.SinkReadPlan planWithoutWatermark(SinkConfig config, String table) {
        SinkConfig copy = copyWithoutWatermark(config);
        return SinkSqlPlanner.plan(table, copy);
    }

    private SinkConfig copyWithoutWatermark(SinkConfig config) {
        SinkConfig copy = new SinkConfig();
        copy.setNodeId(config.getNodeId());
        copy.setTargetType(config.getTargetType());
        copy.setDataSourceId(config.getDataSourceId());
        copy.setTableName(config.getTableName());
        copy.setWriteMode(config.getWriteMode());
        copy.setTableModel(config.getTableModel());
        copy.setTableModelExplicit(config.isTableModelExplicit());
        copy.setKeyColumns(config.getKeyColumns());
        copy.setBatchSize(config.getBatchSize());
        copy.setAutoCreateTable(config.isAutoCreateTable());
        copy.setWatermarkColumn(null);
        copy.setFieldMapping(config.getFieldMapping());
        copy.setTargetLabelOverride(config.getTargetLabelOverride());
        return copy;
    }

    private void advanceWatermark(long flowId, String nodeId, SinkConfig config,
                                  SinkWatermarkEntity existing, Object[] lastRow,
                                  int rowidIndex, int watermarkIndex, String watermarkType,
                                  long committed, LogSink logSink, SinkProgress progress) {
        progress.setWatermarkCommittedRows(committed);
        if (config.getWatermarkColumn() == null || watermarkIndex < 0) {
            return;
        }
        Object value = lastRow[watermarkIndex];
        Object rowid = rowidIndex < 0 ? null : lastRow[rowidIndex];
        if (value == null || rowid == null) {
            logSink.log("WARN", "本批水位线列为空，未推进水位线；失败重试可能重复写入");
            return;
        }
        SinkWatermarkEntity entity = new SinkWatermarkEntity();
        entity.setFlowId(flowId);
        entity.setNodeId(nodeId);
        entity.setTargetKey(config.targetKey());
        entity.setWatermarkColumn(config.getWatermarkColumn());
        entity.setLastValue(stringValue(value));
        entity.setLastValueType(watermarkType);
        entity.setLastRowid(((Number) rowid).longValue());
        entity.setCommittedRows(committed);
        try {
            watermarkRepository.upsert(entity);
        } catch (RuntimeException e) {
            throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                    "Sink 写入失败：推进水位线失败：" + safe(e));
        }
        progress.setWatermarkColumn(entity.getWatermarkColumn());
        progress.setWatermarkValue(entity.getLastValue());
        progress.setWatermarkLastRowid(entity.getLastRowid());
        progress.setWatermarkCommittedRows(entity.getCommittedRows());
    }

    private void initWatermarkProgress(SinkProgress progress, SinkWatermarkEntity watermark) {
        if (watermark == null) {
            return;
        }
        progress.setWatermarkColumn(watermark.getWatermarkColumn());
        progress.setWatermarkValue(watermark.getLastValue());
        progress.setWatermarkLastRowid(watermark.getLastRowid());
        progress.setWatermarkCommittedRows(watermark.getCommittedRows());
    }

    // ------------------------------------------------------------- helpers

    private String upstreamTable(DagNode upstream, long flowId) {
        if ("source".equals(upstream.type())) {
            Long dsId = upstream.longValue("dataSourceId");
            String table = upstream.text("tableName", null);
            if (dsId == null || table == null || table.trim().isEmpty()) {
                throw new BizException(ErrorCode.DAG_INVALID,
                        "上游 Source 节点未配置数据源或表：" + upstream.id(),
                        java.util.Collections.singletonMap("nodeId", upstream.id()));
            }
            return sourceTableNameService.resolve(flowId, dsId, table.trim());
        }
        if ("sink".equals(upstream.type())) {
            throw new BizException(ErrorCode.DAG_INVALID, "Sink 节点不能作为 Sink 的上游",
                    java.util.Collections.singletonMap("nodeId", upstream.id()));
        }
        return nameSanitizer.middleTable(flowId, upstream.id());
    }

    private BizException upstreamNotReady(DagNode node, DagNode upstream, String table) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", node.id());
        data.put("upstreamNodeId", upstream.id());
        data.put("table", table);
        return new BizException(ErrorCode.NODE_NOT_MATERIALIZED,
                "上游数据未物化，请先执行上游节点", data);
    }

    private BizException tableMissing(SinkConfig config) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", config.getNodeId());
        data.put("targetType", config.getTargetType());
        data.put("tableName", config.getTableName());
        return new BizException(ErrorCode.SINK_TABLE_MISSING,
                "目标表不存在且未开启自动建表", data);
    }

    private BizException sinkWriteFailed(SinkConfig config, String label, RuntimeException cause) {
        String detail = cause == null ? "" : "：" + safe(cause);
        return new BizException(ErrorCode.SINK_WRITE_FAILED,
                "Sink 写入失败：目标 " + config.targetLabel() + " 批次 " + label
                        + " Stream Load 重试耗尽" + detail);
    }

    private DataSourceEntity targetDataSource(SinkConfig config) {
        if (config.getDataSourceId() == null) {
            throw SinkConfigParser.invalid(config.getNodeId(), "DATA_SOURCE_REQUIRED",
                    config.isDoris() ? "请选择 Doris 目标数据源（Stream Load 目标）" : "请选择目标数据源");
        }
        DataSourceEntity ds;
        try {
            ds = dataSourceService.getEntity(config.getDataSourceId());
        } catch (BizException e) {
            if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
                throw SinkConfigParser.invalid(config.getNodeId(), "DATA_SOURCE_NOT_FOUND",
                        "目标数据源不存在：" + config.getDataSourceId());
            }
            throw e;
        }
        String dsType = ds.getType() == null ? "" : ds.getType().trim().toUpperCase(Locale.ROOT);
        if (!config.getTargetType().equals(dsType)) {
            throw SinkConfigParser.invalid(config.getNodeId(), "TARGET_TYPE_MISMATCH",
                    "目标类型 " + config.getTargetType() + " 与所选数据源类型 " + dsType + " 不一致");
        }
        if (config.isDoris() && dorisClient.configuredHttpPort(ds) == null) {
            throw SinkConfigParser.invalid(config.getNodeId(), "STREAM_LOAD_PORT_MISSING",
                    DorisClient.STREAM_LOAD_PORT_MISSING_MESSAGE);
        }
        return ds;
    }

    private void fillParams(SinkWriteResult result, SinkConfig config, String upstreamTable) {
        result.getParams().put("targetType", config.getTargetType());
        result.getParams().put("targetTable", config.getTableName());
        result.getParams().put("writeMode", config.getWriteMode());
        if (config.isDoris()) {
            result.getParams().put("tableModel", config.getTableModel());
            result.getParams().put("keyColumns", new ArrayList<>(config.getKeyColumns()));
        }
        result.getParams().put("batchSize", config.getBatchSize());
        result.getParams().put("watermarkColumn", config.getWatermarkColumn());
        result.getParams().put("upstreamTable", upstreamTable);
        List<Map<String, Object>> mapping = new ArrayList<>();
        for (SinkField field : config.getFieldMapping()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("source", field.getSource());
            item.put("target", field.getTarget());
            item.put("type", field.getType());
            mapping.add(item);
        }
        result.getParams().put("fieldMapping", mapping);
    }

    private String insertSql(SinkConfig config) {
        List<String> columns = new ArrayList<>();
        List<String> marks = new ArrayList<>();
        for (SinkField field : config.getFieldMapping()) {
            columns.add(SinkDdlBuilder.column(config.getTargetType(), field.getTarget()));
            marks.add("?");
        }
        return "INSERT INTO " + SinkDdlBuilder.qualified(config.getTargetType(), config.getTableName())
                + " (" + String.join(", ", columns) + ") VALUES (" + String.join(", ", marks) + ")";
    }

    private int[] sourceIndexes(SinkConfig config, SinkSqlPlanner.SinkReadPlan plan) {
        Map<String, Integer> index = new HashMap<>();
        for (int i = 0; i < plan.getReadColumns().size(); i++) {
            index.put(plan.getReadColumns().get(i).toLowerCase(Locale.ROOT), i);
        }
        int[] result = new int[config.getFieldMapping().size()];
        for (int i = 0; i < result.length; i++) {
            Integer idx = index.get(config.getFieldMapping().get(i).getSource().toLowerCase(Locale.ROOT));
            if (idx == null) {
                throw new BizException(ErrorCode.SINK_WRITE_FAILED,
                        "Sink 写入失败：读列中缺少字段 "
                                + config.getFieldMapping().get(i).getSource());
            }
            result[i] = idx;
        }
        return result;
    }

    private int watermarkIndex(SinkConfig config, SinkSqlPlanner.SinkReadPlan plan) {
        if (config.getWatermarkColumn() == null) {
            return -1;
        }
        for (int i = 0; i < plan.getReadColumns().size(); i++) {
            if (plan.getReadColumns().get(i).equalsIgnoreCase(config.getWatermarkColumn())) {
                return i;
            }
        }
        return -1;
    }

    private String typeOf(List<ColumnInfo> columns, String name) {
        if (name == null) {
            return null;
        }
        for (ColumnInfo column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column.getType();
            }
        }
        return null;
    }

    private String labelPrefix(long runId, String nodeId) {
        String safeNode = nodeId == null ? "node" : nodeId.replaceAll("[^A-Za-z0-9_-]", "_");
        String base = "sink_" + runId + "_" + safeNode;
        if (base.length() > 100) {
            base = base.substring(0, 80) + "_" + Integer.toHexString(safeNode.hashCode());
        }
        return base;
    }

    private static Map<String, Object> columnMap(String name, String type) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", name);
        map.put("type", type);
        return map;
    }

    private static String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return DATETIME_FORMAT.format(((Timestamp) value).toLocalDateTime());
        }
        if (value instanceof LocalDateTime) {
            return DATETIME_FORMAT.format((LocalDateTime) value);
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime()).toString();
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        return String.valueOf(value);
    }

    private static Object jsonValue(Object value) {
        if (value == null || value instanceof String || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Timestamp) {
            return DATETIME_FORMAT.format(((Timestamp) value).toLocalDateTime());
        }
        if (value instanceof LocalDateTime) {
            return DATETIME_FORMAT.format((LocalDateTime) value);
        }
        if (value instanceof LocalDate || value instanceof LocalTime) {
            return value.toString();
        }
        if (value instanceof Date || value instanceof Time) {
            return value.toString();
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime()).toString();
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        return String.valueOf(value);
    }

    private static boolean isDuplicateTable(SQLException e) {
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        int code = e.getErrorCode();
        return code == 1050 || code == 955 || code == 1920
                || message.contains("already exists") || message.contains("ora-00955")
                || message.contains("已存在") || message.contains("同名对象");
    }

    private static int backoff(int attempt) {
        return attempt == 1 ? 2000 : 4000;
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void rollbackQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException e) {
            log.warn("回滚 Sink 批插失败：{}", e.getMessage());
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            log.warn("关闭 Sink 目标连接失败：{}", e.getMessage());
        }
    }

    private static String safe(Throwable e) {
        if (e == null) {
            return "";
        }
        String message = e.getMessage();
        return message == null ? e.getClass().getSimpleName()
                : (message.length() > 500 ? message.substring(0, 500) : message);
    }
}
