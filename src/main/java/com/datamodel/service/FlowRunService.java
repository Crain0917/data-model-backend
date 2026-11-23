package com.datamodel.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.FailStage;
import com.datamodel.common.JsonUtils;
import com.datamodel.common.PageResult;
import com.datamodel.common.crypto.AesGcmCipher;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.domain.FlowEntity;
import com.datamodel.domain.FlowRunEntity;
import com.datamodel.domain.NodeRunEntity;
import com.datamodel.domain.RunLogEntity;
import com.datamodel.domain.TableRegistryEntity;
import com.datamodel.dto.FlowRunVO;
import com.datamodel.dto.NodeLogsVO;
import com.datamodel.dto.NodeRunVO;
import com.datamodel.dto.RunLogVO;
import com.datamodel.mapper.FlowMapper;
import com.datamodel.mapper.FlowRunMapper;
import com.datamodel.mapper.NodeRunMapper;
import com.datamodel.mapper.RunLogMapper;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.service.doris.DorisRegistryRepository;
import com.datamodel.service.doris.MaterializeResult;
import com.datamodel.service.doris.SourceSyncService;
import com.datamodel.service.doris.MaterializeService;
import com.datamodel.service.doris.SinkWatermarkRepository;
import com.datamodel.service.doris.SyncResult;
import com.datamodel.sink.SinkProgress;
import com.datamodel.sink.SinkWriteResult;
import com.datamodel.sink.SinkWriteService;
import com.datamodel.sql.Condition;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.SqlBuildResult;
import com.datamodel.sql.SqlBuilderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Service
public class FlowRunService {

    private static final Logger log = LoggerFactory.getLogger(FlowRunService.class);
    private static final int MAX_MESSAGE = 4000;

    private final FlowService flowService;
    private final FlowMapper flowMapper;
    private final FlowRunMapper flowRunMapper;
    private final NodeRunMapper nodeRunMapper;
    private final RunLogMapper runLogMapper;
    private final DataSourceService dataSourceService;
    private final AesGcmCipher aesGcmCipher;
    private final SourceSyncService sourceSyncService;
    private final MaterializeService materializeService;
    private final SqlBuilderFactory sqlBuilderFactory;
    private final SinkWriteService sinkWriteService;
    private final SinkWatermarkRepository sinkWatermarkRepository;
    private final DorisRegistryRepository registryRepository;
    private final DorisClient dorisClient;
    private final ExecutorService executor;

    private final Set<Long> runningFlows = ConcurrentHashMap.newKeySet();

    public FlowRunService(FlowService flowService,
                          FlowMapper flowMapper,
                          FlowRunMapper flowRunMapper,
                          NodeRunMapper nodeRunMapper,
                          RunLogMapper runLogMapper,
                          DataSourceService dataSourceService,
                          AesGcmCipher aesGcmCipher,
                          SourceSyncService sourceSyncService,
                          MaterializeService materializeService,
                          SqlBuilderFactory sqlBuilderFactory,
                          SinkWriteService sinkWriteService,
                          SinkWatermarkRepository sinkWatermarkRepository,
                          DorisRegistryRepository registryRepository,
                          DorisClient dorisClient,
                          @Qualifier("flowRunExecutor") ExecutorService executor) {
        this.flowService = flowService;
        this.flowMapper = flowMapper;
        this.flowRunMapper = flowRunMapper;
        this.nodeRunMapper = nodeRunMapper;
        this.runLogMapper = runLogMapper;
        this.dataSourceService = dataSourceService;
        this.aesGcmCipher = aesGcmCipher;
        this.sourceSyncService = sourceSyncService;
        this.materializeService = materializeService;
        this.sqlBuilderFactory = sqlBuilderFactory;
        this.sinkWriteService = sinkWriteService;
        this.sinkWatermarkRepository = sinkWatermarkRepository;
        this.registryRepository = registryRepository;
        this.dorisClient = dorisClient;
        this.executor = executor;
    }

    public Long startFlowRun(Long flowId) {
        FlowEntity flow = flowService.getEntity(flowId);
        JsonNode dagJson = flowService.parseDag(flow);
        FlowValidation.validate(dagJson);
        Dag dag = Dag.parse(dagJson);
        if (dag.nodeList().isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "画布为空，无法执行");
        }
        List<String> order = FlowValidation.topologicalOrder(dag, dag.nodes().keySet());
        return submit(flow, dagJson, dag, order, "FULL", null, false, null);
    }

    /**
     * 中间任务：不访问源库、不重新 Stream Load，source 节点记录为 SKIPPED，
     * 其余节点按拓扑顺序重跑。前置校验先于创建 run，缺失副本不自动同步。
     */
    public Long startMiddleRun(Long flowId) {
        FlowEntity flow = flowService.getEntity(flowId);
        JsonNode dagJson = flowService.parseDag(flow);
        FlowValidation.validate(dagJson);
        Dag dag = Dag.parse(dagJson);
        if (dag.nodeList().isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "画布为空，无法执行");
        }
        Map<String, String> sourceTables = precheckSourceCopies(flowId, dag);
        List<String> order = FlowValidation.topologicalOrder(dag, dag.nodes().keySet());
        return submit(flow, dagJson, dag, order, "MIDDLE", null, false, sourceTables);
    }

    /**
     * 中间任务前置校验：每个 source 节点都必须有 READY 的 SRC 登记且 Doris 物理表存在。
     * 在创建 run 之前失败，绝不自动同步。
     */
    private Map<String, String> precheckSourceCopies(long flowId, Dag dag) {
        Map<String, String> tables = new LinkedHashMap<>();
        for (DagNode node : dag.nodeList()) {
            if (!"source".equals(node.type())) {
                continue;
            }
            Long dsId = node.longValue("dataSourceId");
            String srcTable = node.text("tableName", null);
            String label = srcTable == null || srcTable.trim().isEmpty() ? "未配置" : srcTable.trim();
            if (dsId == null || srcTable == null || srcTable.trim().isEmpty()) {
                throw sourceCopyMissing(node.id(), label);
            }
            TableRegistryEntity registry = registryRepository.findSrc(flowId, dsId, srcTable.trim());
            if (registry == null || registry.getDorisTable() == null
                    || registry.getDorisTable().trim().isEmpty()
                    || !"READY".equalsIgnoreCase(registry.getStatus())) {
                throw sourceCopyMissing(node.id(), label);
            }
            if (!dorisClient.tableExistsStrict(registry.getDorisTable())) {
                throw sourceCopyMissing(node.id(), label);
            }
            tables.put(node.id(), registry.getDorisTable());
        }
        return tables;
    }

    private static BizException sourceCopyMissing(String nodeId, String table) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", nodeId);
        data.put("table", table);
        data.put("reason", "SOURCE_COPY_MISSING");
        return new BizException(ErrorCode.PARAM_ERROR,
                "源表副本缺失（节点 " + nodeId + " / 表 " + table
                        + "），请先执行『运行任务』完成源表同步", data);
    }

    public Long startNodeRun(Long flowId, String nodeId) {
        return startNodeRun(flowId, nodeId, false);
    }

    /**
     * @param reuseUpstream true 时只执行目标 Sink 节点，直接读已物化的上游表，
     *                      不重算上游、不重分配 __dm_rowid（M3.7 §19.5）。
     */
    public Long startNodeRun(Long flowId, String nodeId, boolean reuseUpstream) {
        FlowEntity flow = flowService.getEntity(flowId);
        JsonNode dagJson = flowService.parseDag(flow);
        FlowValidation.validate(dagJson);
        Dag dag = Dag.parse(dagJson);
        DagNode node = dag.node(nodeId);
        if (node == null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("nodeId", nodeId);
            data.put("reason", "NODE_NOT_FOUND");
            throw new BizException(ErrorCode.DAG_INVALID, "节点不存在：" + nodeId, data);
        }
        if (reuseUpstream && !"sink".equals(node.type())) {
            throw new BizException(ErrorCode.PARAM_ERROR, "仅 Sink 节点支持复用上游数据重试");
        }
        // 预检必须发生在 submit 之前：不创建 run、不占用 running 锁（D-M3.7-06）。
        if (reuseUpstream) {
            sinkWriteService.ensureUpstreamMaterialized(flowId, dagJson, node);
        }
        List<String> order = reuseUpstream
                ? Collections.singletonList(nodeId)
                : FlowValidation.executionOrder(dag, nodeId);
        return submit(flow, dagJson, dag, order, "NODE", nodeId, reuseUpstream, null);
    }

    private Long submit(FlowEntity flow, JsonNode dagJson, Dag dag,
                        List<String> order, String runType, String targetNodeId,
                        boolean reuseUpstream, Map<String, String> middleSourceTables) {
        if (!runningFlows.add(flow.getId())) {
            throw new BizException(ErrorCode.RUN_CONFLICT, "任务正在执行，请等待当前执行结束");
        }
        try {
            Long running = flowRunMapper.selectCount(new LambdaQueryWrapper<FlowRunEntity>()
                    .eq(FlowRunEntity::getFlowId, flow.getId())
                    .in(FlowRunEntity::getStatus, "RUNNING", "PENDING"));
            if (running != null && running > 0) {
                throw new BizException(ErrorCode.RUN_CONFLICT, "任务存在未结束的执行实例");
            }
            int clearedWatermarks = "FULL".equals(runType)
                    ? sinkWatermarkRepository.deleteFlow(flow.getId()) : 0;
            LocalDateTime now = LocalDateTime.now();
            FlowRunEntity run = new FlowRunEntity();
            run.setFlowId(flow.getId());
            run.setFlowName(flow.getName());
            run.setRunType(runType);
            run.setTargetNode(targetNodeId);
            run.setStatus("RUNNING");
            run.setStartTime(now);
            run.setCreateTime(now);
            flowRunMapper.insert(run);

            String runLabel = "NODE".equals(runType) ? "单节点 " + targetNodeId
                    : "MIDDLE".equals(runType) ? "中间任务（不重新同步源表）" : "整任务";
            writeLog(run.getId(), null, "INFO",
                    "开始执行（" + runLabel + "），共 " + order.size() + " 个节点");
            if (middleSourceTables != null && !middleSourceTables.isEmpty()) {
                writeLog(run.getId(), null, "INFO",
                        "中间任务：跳过 " + middleSourceTables.size() + " 个 source 节点的源表同步");
            }
            if (clearedWatermarks > 0) {
                writeLog(run.getId(), null, "INFO",
                        "整任务重跑：已重置 " + clearedWatermarks + " 条 Sink 水位线");
            }

            executor.execute(() -> execute(run, dagJson, dag, order, reuseUpstream,
                    middleSourceTables));
            return run.getId();
        } catch (RuntimeException e) {
            runningFlows.remove(flow.getId());
            throw e;
        }
    }

    private void execute(FlowRunEntity run, JsonNode dagJson, Dag dag, List<String> order,
                         boolean reuseUpstream, Map<String, String> middleSourceTables) {
        long start = System.currentTimeMillis();
        String status = "SUCCESS";
        String error = null;
        try {
            for (String nodeId : order) {
                DagNode node = dag.node(nodeId);
                if (node == null) {
                    continue;
                }
                if (middleSourceTables != null && "source".equals(node.type())) {
                    recordSkippedSource(run, node, middleSourceTables.get(nodeId));
                    continue;
                }
                runNode(run, dagJson, node, reuseUpstream);
            }
        } catch (BizException e) {
            status = "FAILED";
            error = safe(e.getMessage());
            writeLog(run.getId(), null, "ERROR", "执行失败：" + error);
        } catch (Exception e) {
            status = "FAILED";
            error = safe(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            log.error("任务执行异常 flow={} run={}", run.getFlowId(), run.getId(), e);
            writeLog(run.getId(), null, "ERROR", "执行失败：" + error);
        } finally {
            long cost = System.currentTimeMillis() - start;
            LocalDateTime end = LocalDateTime.now();
            FlowRunEntity update = new FlowRunEntity();
            update.setId(run.getId());
            update.setStatus(status);
            update.setEndTime(end);
            update.setCostMs(cost);
            update.setErrMsg(error);
            flowRunMapper.updateById(update);
            flowMapper.update(null, new LambdaUpdateWrapper<FlowEntity>()
                    .eq(FlowEntity::getId, run.getFlowId())
                    .set(FlowEntity::getLastRunStatus, status)
                    .set(FlowEntity::getLastRunTime, end)
                    .set(FlowEntity::getUpdateTime, end));
            writeLog(run.getId(), null, "SUCCESS".equals(status) ? "INFO" : "ERROR",
                    "执行结束，状态 " + status + "，耗时 " + cost + " ms");
            runningFlows.remove(run.getFlowId());
        }
    }

    private void runNode(FlowRunEntity run, JsonNode dagJson, DagNode node, boolean reuseUpstream) {
        LocalDateTime startTime = LocalDateTime.now();
        long start = System.currentTimeMillis();
        NodeRunEntity nr = new NodeRunEntity();
        nr.setFlowRunId(run.getId());
        nr.setFlowId(run.getFlowId());
        nr.setNodeId(node.id());
        nr.setNodeType(node.type());
        nr.setNodeName(node.label() == null || node.label().trim().isEmpty() ? node.id() : node.label());
        nr.setStatus("RUNNING");
        nr.setStartTime(startTime);
        nr.setCreateTime(startTime);
        nodeRunMapper.insert(nr);

        try {
            if ("source".equals(node.type())) {
                runSource(run, node, nr);
            } else if ("sink".equals(node.type())) {
                runSink(run, dagJson, node, nr, reuseUpstream);
            } else {
                runMaterialize(run, dagJson, node, nr);
            }
            if ("RUNNING".equals(nr.getStatus())) {
                nr.setStatus("SUCCESS");
            }
        } catch (BizException e) {
            nr.setStatus("FAILED");
            nr.setErrMsg(safe(e.getMessage()));
            nr.setErrorCode(e.getErrorCode().getCode());
            nr.setFailStage(failStageOf(node, nr.getErrMsg()));
            writeLog(run.getId(), node.id(), "ERROR",
                    "节点失败[" + nr.getFailStage() + "]：" + safe(e.getMessage()));
            throw e;
        } catch (Exception e) {
            nr.setStatus("FAILED");
            nr.setErrMsg(safe(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
            nr.setErrorCode(ErrorCode.INTERNAL_ERROR.getCode());
            nr.setFailStage(failStageOf(node, nr.getErrMsg()));
            writeLog(run.getId(), node.id(), "ERROR",
                    "节点失败[" + nr.getFailStage() + "]：" + nr.getErrMsg());
            throw new BizException(ErrorCode.INTERNAL_ERROR, "节点执行异常：" + nr.getErrMsg());
        } finally {
            LocalDateTime end = LocalDateTime.now();
            nr.setEndTime(end);
            nr.setCostMs(System.currentTimeMillis() - start);
            nodeRunMapper.updateById(nr);
        }
    }

    /** 中间任务：source 节点不访问源库、不 Stream Load，回填已同步副本表并记为 SKIPPED。 */
    private void recordSkippedSource(FlowRunEntity run, DagNode node, String targetTable) {
        LocalDateTime now = LocalDateTime.now();
        NodeRunEntity nr = new NodeRunEntity();
        nr.setFlowRunId(run.getId());
        nr.setFlowId(run.getFlowId());
        nr.setNodeId(node.id());
        nr.setNodeType(node.type());
        nr.setNodeName(node.label() == null || node.label().trim().isEmpty() ? node.id() : node.label());
        nr.setStatus("SKIPPED");
        nr.setTargetTable(targetTable);
        nr.setRowCount(0L);
        nr.setCostMs(0L);
        nr.setSqlText("复用已同步源表 " + targetTable + "（中间任务不重新同步）");
        nr.setStartTime(now);
        nr.setEndTime(now);
        nr.setCreateTime(now);
        nodeRunMapper.insert(nr);
        writeLog(run.getId(), node.id(), "INFO",
                "中间任务：跳过源表同步，复用已同步副本表 " + targetTable);
    }

    private void runSource(FlowRunEntity run, DagNode node, NodeRunEntity nr) {
        Long dsId = node.longValue("dataSourceId");
        String tableName = node.text("tableName", null);
        if (dsId == null || tableName == null || tableName.trim().isEmpty()) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("nodeId", node.id());
            data.put("reason", "CONFIG_INCOMPLETE");
            throw new BizException(ErrorCode.DAG_INVALID,
                    "Source 节点未选择数据源或数据表：" + node.id(), data);
        }
        DataSourceEntity ds;
        try {
            ds = dataSourceService.getEntity(dsId);
        } catch (BizException e) {
            if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
                throw new BizException(ErrorCode.DRIVER_ERROR, "数据源不存在：" + dsId);
            }
            throw e;
        }
        String password = aesGcmCipher.decrypt(ds.getPasswordEnc());
        List<String> columns = node.selectedFieldNames();
        List<Condition> filters = node.sourceFilters();

        writeLog(run.getId(), node.id(), "INFO",
                "同步源表 " + tableName + " → Doris 副本表（" + columns.size() + " 列，过滤条件 "
                        + filters.size() + " 个）");
        SyncResult result = sourceSyncService.sync(ds, password, tableName.trim(), columns, filters,
                run.getFlowId(), run.getId(), node.id());
        nr.setRowCount(result.getLoadedRows());
        nr.setTargetTable(result.getTargetTable());
        nr.setSqlText("Stream Load -> " + result.getTargetTable());
        if (result.isRecreated()) {
            writeLog(run.getId(), node.id(), "WARN", "表结构变更，已重建 Doris 副本表");
        }
        writeLog(run.getId(), node.id(), "INFO",
                "源表同步完成，源行数 " + result.getSourceRows() + "，导入行数 " + result.getLoadedRows());
    }

    private void runMaterialize(FlowRunEntity run, JsonNode dagJson, DagNode node, NodeRunEntity nr) {
        SqlBuildResult built = sqlBuilderFactory.build(dagJson, run.getFlowId(), node.id());
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("sql", built.getSql());
        params.put("params", built.getParams());
        nr.setParams(JsonUtils.toJson(params));
        nr.setSqlText(built.getDisplaySql());
        writeLog(run.getId(), node.id(), "INFO", "生成 SQL：" + safe(built.getDisplaySql()));

        MaterializeResult result = materializeService.materialize(dagJson, run.getFlowId(),
                run.getId(), node);
        nr.setRowCount(result.getRows());
        nr.setTargetTable(result.getTargetTable());
        nr.setSqlText(result.getSql());
        if (result.isRecreated()) {
            writeLog(run.getId(), node.id(), "WARN", "表结构变更，已重建中间表 " + result.getTargetTable());
        }
        writeLog(run.getId(), node.id(), "INFO",
                "物化完成 → " + result.getTargetTable() + "，输出行数 " + result.getRows());
    }

    private void runSink(FlowRunEntity run, JsonNode dagJson, DagNode node, NodeRunEntity nr,
                         boolean reuseUpstream) {
        writeLog(run.getId(), node.id(), "INFO", reuseUpstream
                ? "复用已物化上游，仅执行 Sink 节点（不重算上游）"
                : "执行 Sink 节点");
        SinkProgress progress = new SinkProgress();
        try {
            SinkWriteResult result = sinkWriteService.write(run.getFlowId(), run.getId(), dagJson,
                    node, reuseUpstream,
                    (level, message) -> writeLog(run.getId(), node.id(), level, message), progress);
            nr.setTargetTable(result.getTargetTable());
            nr.setRowCount(result.getRows());
            nr.setSqlText(result.getSqlText());
            nr.setParams(JsonUtils.toJson(result.getParams()));
            writeLog(run.getId(), node.id(), "INFO",
                    "Sink 写入完成 → " + result.getTargetType() + ":" + result.getTargetTable()
                            + "，写入 " + result.getRows() + " 行，共 " + result.getBatches() + " 批");
        } finally {
            // 失败时也必须回写已提交行数与最后成功水位线（D-M3.7-02/D-03）。
            if (nr.getTargetTable() == null) {
                nr.setTargetTable(progress.getTargetTable());
            }
            nr.setRowCount(progress.getRowsWritten());
            if (nr.getSqlText() == null) {
                nr.setSqlText(progress.getSqlText());
            }
            if (!progress.getParams().isEmpty()) {
                nr.setParams(JsonUtils.toJson(progress.getParams()));
            }
            nr.setWriteMode(progress.getWriteMode());
            nr.setWatermarkColumn(progress.getWatermarkColumn());
            nr.setWatermarkValue(progress.getWatermarkValue());
            nr.setWatermarkRowid(progress.getWatermarkLastRowid());
            nr.setCommittedRows(progress.getWatermarkCommittedRows());
            nr.setLastBatchNo(progress.getLastBatchNo());
        }
    }

    private static String failStageOf(DagNode node, String errMsg) {
        if ("sink".equals(node.type())) {
            return FailStage.SINK_WRITE.name();
        }
        return FailStage.classify(errMsg);
    }

    public FlowRunVO getRun(Long runId) {
        FlowRunEntity run = flowRunMapper.selectById(runId);
        if (run == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "执行记录不存在");
        }
        FlowRunVO vo = FlowRunVO.from(run);
        List<NodeRunEntity> nodes = nodeRunMapper.selectList(new LambdaQueryWrapper<NodeRunEntity>()
                .eq(NodeRunEntity::getFlowRunId, runId)
                .orderByAsc(NodeRunEntity::getId));
        vo.setNodes(nodes.stream().map(NodeRunVO::from).collect(Collectors.toList()));
        return vo;
    }

    public PageResult<FlowRunVO> listRuns(Long flowId, Integer page, Integer size) {
        flowService.getEntity(flowId);
        int p = page == null || page < 1 ? 1 : page;
        int s = size == null || size < 1 ? 20 : Math.min(size, 200);
        Page<FlowRunEntity> result = flowRunMapper.selectPage(new Page<>(p, s),
                new LambdaQueryWrapper<FlowRunEntity>()
                        .eq(FlowRunEntity::getFlowId, flowId)
                        .orderByDesc(FlowRunEntity::getId));
        List<FlowRunVO> list = result.getRecords().stream().map(FlowRunVO::from)
                .collect(Collectors.toList());
        return PageResult.of(list, result.getTotal());
    }

    public NodeLogsVO nodeLogs(Long runId, String nodeId) {
        FlowRunEntity run = flowRunMapper.selectById(runId);
        if (run == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "执行记录不存在");
        }
        NodeRunEntity nr = nodeRunMapper.selectOne(new LambdaQueryWrapper<NodeRunEntity>()
                .eq(NodeRunEntity::getFlowRunId, runId)
                .eq(NodeRunEntity::getNodeId, nodeId)
                .last("LIMIT 1"));
        if (nr == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "该节点没有执行记录");
        }
        List<RunLogEntity> logs = runLogMapper.selectList(new LambdaQueryWrapper<RunLogEntity>()
                .eq(RunLogEntity::getFlowRunId, runId)
                .eq(RunLogEntity::getNodeId, nodeId)
                .orderByAsc(RunLogEntity::getId));
        NodeLogsVO vo = new NodeLogsVO();
        vo.setSqlText(nr.getSqlText());
        vo.setSqlParams(nr.getParams());
        vo.setList(logs.stream().map(RunLogVO::from).collect(Collectors.toList()));
        return vo;
    }

    public void writeLog(Long runId, String nodeId, String level, String message) {
        RunLogEntity entity = new RunLogEntity();
        entity.setFlowRunId(runId);
        entity.setNodeId(nodeId);
        entity.setLevel(level);
        entity.setMessage(safe(message));
        entity.setCreateTime(LocalDateTime.now());
        runLogMapper.insert(entity);
    }

    private static String safe(String message) {
        if (message == null) {
            return null;
        }
        return message.length() > MAX_MESSAGE ? message.substring(0, MAX_MESSAGE) : message;
    }
}
