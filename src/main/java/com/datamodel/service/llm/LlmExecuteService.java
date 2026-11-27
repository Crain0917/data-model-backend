package com.datamodel.service.llm;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.JsonUtils;
import com.datamodel.config.AppProperties;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.datamodel.service.doris.MaterializeResult;
import com.datamodel.service.doris.MaterializeService;
import com.datamodel.service.doris.StreamLoadResult;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.Identifiers;
import com.datamodel.sql.OutputColumn;
import com.datamodel.sql.SqlBuilderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 大模型算子执行：keyset 按上游 __dm_rowid 分批调用模型，
 * 结果写 dm_llm_..._map，全部成功后一次性 JOIN 物化，保证无部分结果。
 */
@Service
public class LlmExecuteService {

    private static final Logger log = LoggerFactory.getLogger(LlmExecuteService.class);
    private static final String ROWID = "__dm_rowid";
    private static final int QUERY_TIMEOUT_SECONDS = 300;
    // Doris 4.1.3 要求 $ 前缀；裸列名会报 empty json path 并过滤全部行
    private static final List<String> MAP_JSON_PATHS = List.of("$.dm_vid", "$.dm_value");
    private static final int STREAM_LOAD_ATTEMPTS = 2;
    private static final int STREAM_LOAD_TIMEOUT_SECONDS = 60;
    private static final long STREAM_LOAD_RETRY_MILLIS = 500L;

    private final AppProperties appProperties;
    private final LlmCredentialService credentialService;
    private final LlmClient llmClient;
    private final LlmResponseParser responseParser = new LlmResponseParser();
    private final DorisClient dorisClient;
    private final DorisNameSanitizer nameSanitizer;
    private final SqlBuilderFactory sqlBuilderFactory;
    private final MaterializeService materializeService;

    public LlmExecuteService(AppProperties appProperties,
                             LlmCredentialService credentialService,
                             LlmClient llmClient,
                             DorisClient dorisClient,
                             DorisNameSanitizer nameSanitizer,
                             SqlBuilderFactory sqlBuilderFactory,
                             MaterializeService materializeService) {
        this.appProperties = appProperties;
        this.credentialService = credentialService;
        this.llmClient = llmClient;
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
        this.sqlBuilderFactory = sqlBuilderFactory;
        this.materializeService = materializeService;
    }

    /** 运行期日志出口：level + message（不含 Key）。 */
    @FunctionalInterface
    public interface LlmLog {
        void log(String level, String message);
    }

    public LlmRunResult run(JsonNode dagJson, long flowId, DagNode node, LlmLog logger) {
        long started = System.currentTimeMillis();
        LlmConfig config = LlmConfig.parse(node);
        Dag dag = Dag.parse(dagJson);
        DagNode upstream = dag.firstUpstream(node.id());
        if (upstream == null) {
            throw failure(config, "LLM_UPSTREAM_MISSING", "大模型节点没有上游节点", null);
        }
        String upstreamTable = sqlBuilderFactory.materializedTable(dagJson, flowId, upstream.id());
        List<OutputColumn> upstreamColumns = sqlBuilderFactory.nodeColumns(dagJson, flowId,
                upstream.id());
        if (upstreamTable == null || upstreamColumns.isEmpty()) {
            throw failure(config, "LLM_UPSTREAM_MISSING",
                    "大模型节点的上游未物化或字段不可用，请先执行上游节点", null);
        }
        config.validate(upstreamColumns);
        llmClient.checkEndpoint(config);

        String apiKey = credentialService.plainKey(flowId, node.id());
        if (apiKey == null || apiKey.isEmpty()) {
            throw new BizException(ErrorCode.LLM_ERROR, "大模型节点未配置 API Key",
                    data(node.id(), "LLM_KEY_MISSING"));
        }
        String mask = credentialService.maskOf(flowId, node.id());
        String mapTable = nameSanitizer.llmMapTable(flowId, node.id());
        String targetTable = nameSanitizer.middleTable(flowId, node.id());
        String runToken = runToken();
        LlmBatchPlanner planner = new LlmBatchPlanner(appProperties.getLlm().getMaxInputChars());
        Counters counters = new Counters();
        List<String> fetchColumns = resolveFields(upstreamColumns, config.fetchFields());

        logger.log("INFO", "大模型节点开始：endpoint=" + config.endpoint() + "，model=" + config.model()
                + "，Key=" + (mask == null ? "****" : mask) + "，批大小=" + config.batchRows()
                + "，模式=" + (config.rowMode() ? "逐行（每行一次请求）" : "批量（${input_text}）")
                + "，上游=" + upstreamTable);

        dorisClient.dropTable(mapTable);
        createMapTable(mapTable);
        try {
            long lastId = 0L;
            int page = 0;
            while (true) {
                List<LlmRow> rows = fetchRows(upstreamTable, fetchColumns, lastId,
                        config.batchRows());
                if (rows.isEmpty()) {
                    break;
                }
                page++;
                lastId = rows.get(rows.size() - 1).id();
                counters.pages++;
                logger.log("INFO", "读取上游第 " + page + " 页：dm_vid "
                        + rows.get(0).id() + "–" + lastId + "，共 " + rows.size() + " 行");

                Map<Long, String> values = new LinkedHashMap<>();
                processRows(rows, config, apiKey, planner, mask, values, counters, logger);
                writeMap(mapTable, values, flowId, node.id(), page, runToken, counters);
                if (rows.size() < config.batchRows()) {
                    break;
                }
            }

            List<OutputColumn> outputColumns = new ArrayList<>(upstreamColumns);
            outputColumns.add(new OutputColumn(Identifiers.requireField(config.outputField()), "STRING"));
            String select = finalSelect(upstreamTable, upstreamColumns, mapTable, config);
            MaterializeResult result = materializeService.materializeQuery(flowId, node.id(),
                    targetTable, outputColumns, select, select, new ArrayList<>());

            dorisClient.dropTable(mapTable);
            counters.message(logger);
            logger.log("INFO", "大模型节点完成：物化 " + result.getTargetTable() + "，输出 "
                    + result.getRows() + " 行，模型调用 " + counters.calls + " 次，耗时 "
                    + (System.currentTimeMillis() - started) + " ms");
            return new LlmRunResult(result.getTargetTable(), result.getRows(), result.isRecreated(),
                    result.getSql(), counters.batches, counters.calls);
        } catch (LlmException e) {
            counters.message(logger);
            throw toBizException(config, e, null);
        } catch (BizException e) {
            counters.message(logger);
            throw e;
        } catch (RuntimeException e) {
            counters.message(logger);
            throw e;
        }
    }

    private void processRows(List<LlmRow> rows, LlmConfig config, String apiKey,
                             LlmBatchPlanner planner, String mask, Map<Long, String> values,
                             Counters counters, LlmLog logger) {
        LlmBatchPlanner.Plan plan = planner.plan(rows, config);
        for (LlmRow row : plan.overlong()) {
            values.put(row.id(), config.unknownValue());
            counters.rowTooLong++;
            logger.log("WARN", "单行文本超过 " + appProperties.getLlm().getMaxInputChars()
                    + " 字符，已置为“" + config.unknownValue() + "”：dm_vid=" + row.id());
        }
        for (LlmBatchPlanner.Batch batch : plan.batches()) {
            processBatch(batch, config, apiKey, planner, mask, values, counters, logger);
        }
    }

    private void processBatch(LlmBatchPlanner.Batch batch, LlmConfig config, String apiKey,
                              LlmBatchPlanner planner, String mask, Map<Long, String> values,
                              Counters counters, LlmLog logger) {
        long begin = System.currentTimeMillis();
        LlmClient.ChatResult result;
        try {
            counters.calls++;
            result = llmClient.chat(config, apiKey, batch.prompt(), config.timeoutSeconds(),
                    config.maxRetries());
        } catch (LlmException e) {
            if ("LLM_ROW_TOO_LONG".equals(e.getReason()) && batch.rows().size() > 1) {
                logger.log("WARN", "模型上下文超限，对半拆批重试：dm_vid "
                        + batch.minId() + "–" + batch.maxId());
                int mid = batch.rows().size() / 2;
                processRows(new ArrayList<>(batch.rows().subList(0, mid)), config, apiKey, planner,
                        mask, values, counters, logger);
                processRows(new ArrayList<>(batch.rows().subList(mid, batch.rows().size())), config,
                        apiKey, planner, mask, values, counters, logger);
                return;
            }
            if ("LLM_ROW_TOO_LONG".equals(e.getReason())) {
                LlmRow row = batch.rows().get(0);
                values.put(row.id(), config.unknownValue());
                counters.rowTooLong++;
                logger.log("WARN", "单行超出模型上下文限制，已置为“" + config.unknownValue()
                        + "”：dm_vid=" + row.id());
                return;
            }
            throw toBizException(config, e, batch);
        }

        LlmResponseParser.ParsedBatch parsed;
        try {
            parsed = parseWithRetry(result.content(), batch, config, apiKey, counters, logger);
        } catch (LlmException e) {
            throw toBizException(config, e, batch);
        }
        values.putAll(parsed.values());
        counters.batches++;
        counters.missing += parsed.missing();
        counters.invalid += parsed.invalid();
        counters.duplicate += parsed.duplicate();
        logger.log("INFO", "第 " + counters.batches + " 批完成：dm_vid " + batch.minId() + "–"
                + batch.maxId() + "，行数 " + batch.rows().size() + "，tokens "
                + result.totalTokens() + "，耗时 " + (System.currentTimeMillis() - begin) + " ms");
        if (parsed.missing() > 0 || parsed.invalid() > 0 || parsed.duplicate() > 0) {
            logger.log("WARN", "本批结果需修正：缺失 " + parsed.missing() + " 行、非法值 "
                    + parsed.invalid() + " 行、重复 id " + parsed.duplicate() + " 行（重复取后者），"
                    + "缺失/非法已置为“" + config.unknownValue() + "”");
        }
    }

    private LlmResponseParser.ParsedBatch parseWithRetry(String content, LlmBatchPlanner.Batch batch,
                                                         LlmConfig config, String apiKey,
                                                         Counters counters, LlmLog logger) {
        List<Long> ids = new ArrayList<>(batch.rows().size());
        for (LlmRow row : batch.rows()) {
            ids.add(row.id());
        }
        try {
            return responseParser.parse(content, ids, config);
        } catch (LlmException first) {
            logger.log("WARN", "模型响应解析失败，追加一次“只输出 JSON 数组”重试：dm_vid "
                    + batch.minId() + "–" + batch.maxId() + "；响应片段："
                    + snippet(content));
            String prompt = batch.prompt()
                    + "\n\n只输出 JSON 数组，格式为 [{\"id\":\"...\",\"value\":\"...\"}]，不要包含任何解释或代码块标记。";
            counters.calls++;
            LlmClient.ChatResult retry = llmClient.chat(config, apiKey, prompt,
                    config.timeoutSeconds(), config.maxRetries());
            try {
                return responseParser.parse(retry.content(), ids, config);
            } catch (LlmException second) {
                logger.log("WARN", "解析失败重试仍非 JSON 数组；响应片段：" + snippet(retry.content()));
                throw new LlmException("LLM_PARSE_FAILED",
                        "模型返回内容无法解析为 JSON 数组（重试 1 次后仍失败）",
                        second.getHttpStatus());
            }
        }
    }

    /** 输入字段（逻辑名）→ 上游实际列名；顺序与 inputFields 一致。 */
    private static List<String> resolveFields(List<OutputColumn> upstreamColumns,
                                              List<String> inputFields) {
        List<String> actual = new ArrayList<>(inputFields.size());
        for (String field : inputFields) {
            OutputColumn column = LlmConfig.findColumn(upstreamColumns, field);
            if (column == null) {
                throw new IllegalStateException("输入字段不存在：" + field);
            }
            actual.add(column.getName());
        }
        return actual;
    }

    /** actualColumns 为上游实际列名；返回行的 fields/values 与该列表严格同序。 */
    private List<LlmRow> fetchRows(String table, List<String> actualColumns, long afterId,
                                   int batchRows) {
        StringBuilder sql = new StringBuilder("SELECT `__dm_rowid`");
        for (String field : actualColumns) {
            sql.append(", ").append(Identifiers.quote(field, Identifiers.QuoteStyle.BACKTICK));
        }
        sql.append(" FROM ").append(dorisClient.qualifiedQuoted(table))
                .append(" WHERE `__dm_rowid` > ? ORDER BY `__dm_rowid` ASC LIMIT ?");
        List<LlmRow> rows = new ArrayList<>(batchRows);
        try (Connection connection = dorisClient.open();
             PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setLong(1, afterId);
            ps.setInt(2, batchRows);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong(1);
                    List<String> values = new ArrayList<>(actualColumns.size());
                    for (int i = 0; i < actualColumns.size(); i++) {
                        String value = rs.getString(i + 2);
                        values.add(value == null ? "" : value);
                    }
                    rows.add(new LlmRow(id, actualColumns, values));
                }
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                    "读取大模型节点上游失败：" + safe(e.getMessage()),
                    data(null, "LLM_UPSTREAM_MISSING"));
        }
        return rows;
    }

    private void createMapTable(String mapTable) {
        String ddl = "CREATE TABLE " + dorisClient.qualified(mapTable) + " (\n"
                + "  `dm_vid` BIGINT NOT NULL,\n"
                + "  `dm_value` STRING NULL\n"
                + ") DUPLICATE KEY(`dm_vid`) DISTRIBUTED BY HASH(`dm_vid`) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")";
        dorisClient.createTable(ddl, mapTable);
    }

    /** 映射表写入改 Stream Load（JSON Lines + jsonpaths），label 重试复用、行数校验。 */
    private void writeMap(String mapTable, Map<Long, String> values, long flowId, String nodeId,
                          int batchIndex, String runToken, Counters counters) {
        if (values.isEmpty()) {
            return;
        }
        byte[] body = mapBody(values);
        String label = mapLabel(flowId, nodeId, batchIndex, runToken);
        int expected = values.size();
        BizException last = null;
        for (int attempt = 1; attempt <= STREAM_LOAD_ATTEMPTS; attempt++) {
            try {
                StreamLoadResult result = dorisClient.streamLoad(mapTable, body, label,
                        MAP_JSON_PATHS, STREAM_LOAD_TIMEOUT_SECONDS);
                checkLoadedRows(result, expected, label);
                counters.mappedRows += expected;
                counters.streamLoads++;
                return;
            } catch (BizException e) {
                last = e;
                log.warn("大模型映射表 Stream Load 失败（第 {}/{} 次）label={}：{}", attempt,
                        STREAM_LOAD_ATTEMPTS, label, safe(e.getMessage()));
                if (attempt < STREAM_LOAD_ATTEMPTS) {
                    sleepQuietly(STREAM_LOAD_RETRY_MILLIS);
                }
            }
        }
        throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                "写入大模型结果映射表失败（label=" + label + "）："
                        + (last == null ? "unknown" : safe(last.getMessage())));
    }

    static byte[] mapBody(Map<Long, String> values) {
        StringBuilder body = new StringBuilder(values.size() * 64);
        for (Map.Entry<Long, String> entry : values.entrySet()) {
            ObjectNode item = JsonUtils.mapper().createObjectNode();
            item.put("dm_vid", entry.getKey());
            item.put("dm_value", entry.getValue() == null ? "" : entry.getValue());
            body.append(item).append('\n');
        }
        return body.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Doris label 在库内保留一段时间，同任务重复执行若复用同一 label 会被拒绝；
     * 因此按“形如 dmllm_{flow}_{node}_{batch}”再追加本次执行标记，只保证同一批次的重试复用。
     */
    static String mapLabel(long flowId, String nodeId, int batchIndex, String runToken) {
        String safeNode = nodeId == null ? "node" : nodeId.replaceAll("[^A-Za-z0-9_]", "_");
        return "dmllm_" + flowId + "_" + safeNode + "_" + batchIndex + "_" + runToken;
    }

    static String runToken() {
        return Long.toString(System.currentTimeMillis(), 36)
                + UUID.randomUUID().toString().substring(0, 4);
    }

    /** Doris 单次 load 原子生效；行数不符即失败，避免静默丢行。 */
    static void checkLoadedRows(StreamLoadResult result, int expected, String label) {
        if (result.getLoadedRows() == expected) {
            return;
        }
        String message = result.getMessage() == null ? "" : result.getMessage();
        if (message.contains("Label Already Exists")) {
            // 重试命中上一次已提交的 label：数据已按该 label 原子落库
            return;
        }
        throw new BizException(ErrorCode.DORIS_EXEC_FAILED,
                "写入大模型结果映射表行数不匹配：预期 " + expected + " 行，实际 "
                        + result.getLoadedRows() + " 行（label=" + label + "）");
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String finalSelect(String upstreamTable, List<OutputColumn> upstreamColumns,
                               String mapTable, LlmConfig config) {
        StringBuilder select = new StringBuilder("SELECT\n  ");
        for (int i = 0; i < upstreamColumns.size(); i++) {
            if (i > 0) {
                select.append(",\n  ");
            }
            select.append("s.").append(Identifiers.quote(upstreamColumns.get(i).getName(),
                    Identifiers.QuoteStyle.BACKTICK));
        }
        select.append(",\n  COALESCE(m.`dm_value`, '")
                .append(config.unknownValue().replace("'", "''"))
                .append("') AS ").append(Identifiers.quote(
                        Identifiers.requireField(config.outputField()),
                        Identifiers.QuoteStyle.BACKTICK));
        select.append("\nFROM ").append(dorisClient.qualifiedQuoted(upstreamTable)).append(" s")
                .append("\nLEFT JOIN ").append(dorisClient.qualifiedQuoted(mapTable))
                .append(" m ON s.`__dm_rowid` = m.`dm_vid`");
        return select.toString();
    }

    /**
     * 数据预览：只读、不建表、不写 map；最多 previewMaxRows 行、不重试、独立超时。
     */
    public PreviewResult preview(JsonNode dagJson, long flowId, DagNode node, int requestedLimit) {
        LlmConfig config = LlmConfig.parse(node);
        Dag dag = Dag.parse(dagJson);
        DagNode upstream = dag.firstUpstream(node.id());
        if (upstream == null) {
            throw failure(config, "LLM_UPSTREAM_MISSING", "大模型节点没有上游节点", null);
        }
        String upstreamTable = sqlBuilderFactory.materializedTable(dagJson, flowId, upstream.id());
        List<OutputColumn> upstreamColumns = sqlBuilderFactory.nodeColumns(dagJson, flowId,
                upstream.id());
        if (upstreamTable == null || upstreamColumns.isEmpty()) {
            throw failure(config, "LLM_UPSTREAM_MISSING",
                    "大模型节点的上游未物化或字段不可用，请先执行上游节点", null);
        }
        config.validate(upstreamColumns);
        llmClient.checkEndpoint(config);
        String apiKey = credentialService.plainKey(flowId, node.id());
        if (apiKey == null || apiKey.isEmpty()) {
            throw failure(config, "LLM_KEY_MISSING", "大模型节点未配置 API Key", null);
        }
        int limit = Math.max(1, Math.min(requestedLimit,
                Math.max(1, appProperties.getLlm().getPreviewMaxRows())));
        // 预览取上游全部列，保证返回 rows 与 columns 一一对应；Prompt 仍只使用输入变量。
        List<String> allColumns = new ArrayList<>(upstreamColumns.size());
        for (OutputColumn column : upstreamColumns) {
            allColumns.add(column.getName());
        }
        List<LlmRow> rows = fetchRows(upstreamTable, allColumns, 0L, limit);
        List<OutputColumn> columns = new ArrayList<>(upstreamColumns);
        columns.add(new OutputColumn(Identifiers.requireField(config.outputField()), "STRING"));

        Map<Long, String> values = new LinkedHashMap<>();
        int calls = 0;
        int batches = 0;
        if (!rows.isEmpty()) {
            LlmBatchPlanner planner = new LlmBatchPlanner(appProperties.getLlm().getMaxInputChars());
            int timeout = Math.max(1, appProperties.getLlm().getPreviewTimeoutSeconds());
            int chunkSize = Math.max(1, config.batchRows());
            // 先按用户填写的“单次批量行数”切块，每块再交给 planner 按字符上限细分。
            for (int start = 0; start < rows.size(); start += chunkSize) {
                List<LlmRow> chunk = new ArrayList<>(rows.subList(start,
                        Math.min(start + chunkSize, rows.size())));
                LlmBatchPlanner.Plan plan = planner.plan(chunk, config);
                for (LlmRow row : plan.overlong()) {
                    values.put(row.id(), config.unknownValue());
                }
                for (LlmBatchPlanner.Batch batch : plan.batches()) {
                    List<Long> ids = new ArrayList<>();
                    for (LlmRow row : batch.rows()) {
                        ids.add(row.id());
                    }
                    try {
                        calls++;
                        LlmClient.ChatResult result = llmClient.chat(config, apiKey, batch.prompt(),
                                timeout, 0);
                        LlmResponseParser.ParsedBatch parsed = responseParser.parse(result.content(),
                                ids, config);
                        values.putAll(parsed.values());
                    } catch (LlmException e) {
                        throw toBizException(config, e, batch);
                    }
                    batches++;
                }
            }
        }
        List<List<Object>> outRows = new ArrayList<>(rows.size());
        for (LlmRow row : rows) {
            List<Object> out = new ArrayList<>(row.values().size() + 1);
            out.addAll(row.values());
            out.add(values.getOrDefault(row.id(), config.unknownValue()));
            outRows.add(out);
        }
        return new PreviewResult(columns, outRows, calls, batches);
    }

    private BizException toBizException(LlmConfig config, LlmException e,
                                        LlmBatchPlanner.Batch batch) {
        Map<String, Object> data = data(config.nodeId(), e.getReason());
        if (batch != null) {
            data.put("batchMinId", batch.minId());
            data.put("batchMaxId", batch.maxId());
        }
        if (e.getHttpStatus() != null) {
            data.put("httpStatus", e.getHttpStatus());
        }
        String range = batch == null ? "" : "（批次 dm_vid " + batch.minId() + "–"
                + batch.maxId() + "）";
        return new BizException(ErrorCode.LLM_ERROR, e.getMessage() + range, data);
    }

    private BizException failure(LlmConfig config, String reason, String message,
                                 LlmBatchPlanner.Batch batch) {
        Map<String, Object> data = data(config.nodeId(), reason);
        if (batch != null) {
            data.put("batchMinId", batch.minId());
            data.put("batchMaxId", batch.maxId());
        }
        return new BizException(ErrorCode.LLM_ERROR, message, data);
    }

    private static Map<String, Object> data(String nodeId, String reason) {
        Map<String, Object> data = new LinkedHashMap<>();
        if (nodeId != null) {
            data.put("nodeId", nodeId);
        }
        data.put("reason", reason);
        return data;
    }

    private static String snippet(String content) {
        if (content == null) {
            return "";
        }
        String oneLine = content.replace('\n', ' ').trim();
        return oneLine.length() > 500 ? oneLine.substring(0, 500) + "…" : oneLine;
    }

    private static String safe(String message) {
        return message == null ? "unknown" : (message.length() > 500
                ? message.substring(0, 500) : message);
    }

    private static final class Counters {
        int pages;
        int batches;
        int calls;
        int missing;
        int invalid;
        int duplicate;
        int rowTooLong;
        int streamLoads;
        long mappedRows;

        void message(LlmLog logger) {
            logger.log("INFO", "大模型统计：读取 " + pages + " 页，调用 " + calls + " 次，成功批 "
                    + batches + " 个，Stream Load " + streamLoads + " 次，映射 " + mappedRows
                    + " 行；缺失 " + missing + "、非法 " + invalid + "、重复 " + duplicate
                    + "、单行超长 " + rowTooLong);
        }
    }

    public record LlmRunResult(String targetTable, long rows, boolean recreated, String sqlText,
                               int batches, int calls) {
    }

    public record PreviewResult(List<OutputColumn> columns, List<List<Object>> rows, int calls,
                                int batches) {
    }
}
