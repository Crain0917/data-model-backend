package com.datamodel.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.PageResult;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.domain.FlowEntity;
import com.datamodel.domain.FlowRunEntity;
import com.datamodel.domain.NodeRunEntity;
import com.datamodel.domain.RunLogEntity;
import com.datamodel.domain.SinkWatermarkEntity;
import com.datamodel.domain.SyncLogEntity;
import com.datamodel.domain.TableRegistryEntity;
import com.datamodel.dto.FlowCreateRequest;
import com.datamodel.dto.FlowDeleteResult;
import com.datamodel.dto.FlowUpdateRequest;
import com.datamodel.dto.FlowUpdateResult;
import com.datamodel.dto.FlowVO;
import com.datamodel.mapper.FlowMapper;
import com.datamodel.mapper.FlowRunMapper;
import com.datamodel.mapper.NodeRunMapper;
import com.datamodel.mapper.RunLogMapper;
import com.datamodel.mapper.SyncLogMapper;
import com.datamodel.mapper.TableRegistryMapper;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.datamodel.service.doris.SinkWatermarkRepository;
import com.datamodel.sink.SinkConfig;
import com.datamodel.sink.SinkConfigParser;
import com.datamodel.sink.SinkField;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.OutputColumn;
import com.datamodel.sql.SqlBuilderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class FlowService {

    private static final Logger log = LoggerFactory.getLogger(FlowService.class);

    private final FlowMapper flowMapper;
    private final FlowRunMapper flowRunMapper;
    private final NodeRunMapper nodeRunMapper;
    private final RunLogMapper runLogMapper;
    private final SyncLogMapper syncLogMapper;
    private final TableRegistryMapper tableRegistryMapper;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final DorisClient dorisClient;
    private final DorisNameSanitizer nameSanitizer;
    private final SinkConfigParser sinkConfigParser;
    private final SinkWatermarkRepository sinkWatermarkRepository;
    private final DataSourceService dataSourceService;
    private final SqlBuilderFactory sqlBuilderFactory;

    public FlowService(FlowMapper flowMapper, FlowRunMapper flowRunMapper,
                       NodeRunMapper nodeRunMapper, RunLogMapper runLogMapper,
                       SyncLogMapper syncLogMapper, TableRegistryMapper tableRegistryMapper,
                       JdbcTemplate jdbcTemplate, ObjectMapper objectMapper,
                       DorisClient dorisClient, DorisNameSanitizer nameSanitizer,
                       SinkConfigParser sinkConfigParser,
                       SinkWatermarkRepository sinkWatermarkRepository,
                       DataSourceService dataSourceService,
                       SqlBuilderFactory sqlBuilderFactory) {
        this.flowMapper = flowMapper;
        this.flowRunMapper = flowRunMapper;
        this.nodeRunMapper = nodeRunMapper;
        this.runLogMapper = runLogMapper;
        this.syncLogMapper = syncLogMapper;
        this.tableRegistryMapper = tableRegistryMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
        this.sinkConfigParser = sinkConfigParser;
        this.sinkWatermarkRepository = sinkWatermarkRepository;
        this.dataSourceService = dataSourceService;
        this.sqlBuilderFactory = sqlBuilderFactory;
    }

    public PageResult<FlowVO> list(String keyword, Integer page, Integer size) {
        int p = page == null || page < 1 ? 1 : page;
        int s = size == null || size < 1 ? 20 : Math.min(size, 200);
        LambdaQueryWrapper<FlowEntity> wrapper = new LambdaQueryWrapper<>();
        if (StringUtils.hasText(keyword)) {
            wrapper.like(FlowEntity::getName, keyword.trim());
        }
        wrapper.orderByDesc(FlowEntity::getUpdateTime).orderByDesc(FlowEntity::getId);
        Page<FlowEntity> result = flowMapper.selectPage(new Page<>(p, s), wrapper);
        List<FlowVO> list = result.getRecords().stream()
                .map(FlowVO::summary)
                .collect(Collectors.toList());
        return PageResult.of(list, result.getTotal());
    }

    @Transactional
    public Long create(FlowCreateRequest request) {
        String name = request.getName() == null ? "" : request.getName().trim();
        if (name.isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "任务名称不能为空");
        }
        Long count = flowMapper.selectCount(new LambdaQueryWrapper<FlowEntity>()
                .eq(FlowEntity::getName, name));
        if (count != null && count > 0) {
            throw new BizException(ErrorCode.CONFLICT, "任务名称已存在");
        }
        LocalDateTime now = LocalDateTime.now();
        FlowEntity entity = new FlowEntity();
        entity.setName(name);
        entity.setDescription(StringUtils.hasText(request.getDescription())
                ? request.getDescription().trim() : null);
        entity.setDagJson("{\"nodes\":[],\"edges\":[]}");
        entity.setNodeCount(0);
        entity.setCreateTime(now);
        entity.setUpdateTime(now);
        try {
            flowMapper.insert(entity);
        } catch (DuplicateKeyException e) {
            throw new BizException(ErrorCode.CONFLICT, "任务名称已存在");
        }
        log.info("新建任务 id={}, name={}", entity.getId(), entity.getName());
        return entity.getId();
    }

    public FlowVO detail(Long id) {
        FlowEntity entity = getEntity(id);
        FlowVO vo = FlowVO.summary(entity);
        vo.setDagJson(parseDag(entity));
        return vo;
    }

    /** M3.7 §21.1：仅更新名称/描述，不动画布、节点数、执行状态。 */
    @Transactional
    public FlowUpdateResult update(Long id, FlowUpdateRequest request) {
        FlowEntity entity = getEntity(id);
        String name = request == null || request.getName() == null ? "" : request.getName().trim();
        if (name.isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "任务名称不能为空");
        }
        if (name.length() < 2 || name.length() > 100) {
            throw new BizException(ErrorCode.PARAM_ERROR, "任务名称长度需为 2–100 个字符");
        }
        String description = request.getDescription() == null ? null : request.getDescription().trim();
        if (description != null && description.length() > 512) {
            throw new BizException(ErrorCode.PARAM_ERROR, "任务描述不能超过 512 个字符");
        }
        Long duplicate = flowMapper.selectCount(new LambdaQueryWrapper<FlowEntity>()
                .eq(FlowEntity::getName, name)
                .ne(FlowEntity::getId, id));
        if (duplicate != null && duplicate > 0) {
            throw new BizException(ErrorCode.CONFLICT, "任务名称已存在");
        }
        entity.setName(name);
        entity.setDescription(StringUtils.hasText(description) ? description : null);
        entity.setUpdateTime(LocalDateTime.now());
        try {
            flowMapper.updateById(entity);
        } catch (DuplicateKeyException e) {
            throw new BizException(ErrorCode.CONFLICT, "任务名称已存在");
        }
        log.info("更新任务信息 id={}, name={}", id, name);
        return FlowUpdateResult.from(entity);
    }

    @Transactional
    public void saveDag(Long id, JsonNode dagJson) {
        FlowEntity entity = getEntity(id);
        if (dagJson == null || !dagJson.isObject()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "dagJson 不能为空");
        }
        FlowValidation.validate(dagJson);
        validateSinkConfigs(id, dagJson);
        JsonNode nodes = dagJson.get("nodes");
        entity.setDagJson(dagJson.toString());
        entity.setNodeCount(nodes == null ? 0 : nodes.size());
        entity.setUpdateTime(LocalDateTime.now());
        flowMapper.updateById(entity);
        log.info("保存画布 flow={} nodes={}", id, entity.getNodeCount());
        reconcileMiddleTables(id, dagJson);
    }

    /**
     * 保存画布时对账清理：删除不再属于当前 DAG 的 dm_mid_{flowId}_*（含历史孤儿表）。
     * 节点仍存在则保留其表；Doris 不可达只记 WARN，不阻断用户保存。
     */
    private void reconcileMiddleTables(Long flowId, JsonNode dagJson) {
        Dag dag;
        try {
            dag = Dag.parse(dagJson);
        } catch (RuntimeException e) {
            log.warn("保存画布对账跳过 flow={}：DAG 无法解析：{}", flowId, e.getMessage());
            return;
        }
        Set<String> keep = new LinkedHashSet<>();
        Set<String> sinkNodeIds = new LinkedHashSet<>();
        for (DagNode node : dag.nodeList()) {
            keep.add(nameSanitizer.middleTable(flowId, node.id()));
            if ("sink".equals(node.type())) {
                sinkNodeIds.add(node.id());
            }
        }
        List<String> existing;
        try {
            existing = dorisClient.listTablesWithPrefix(nameSanitizer.middleTablePrefix(flowId));
        } catch (RuntimeException e) {
            log.warn("保存画布读取中间表清单失败 flow={}，编辑已保存：{}", flowId, e.getMessage());
            return;
        }
        for (String table : existing) {
            if (keep.contains(table)) {
                continue;
            }
            try {
                dorisClient.dropTable(table);
                log.info("保存画布清理已删除节点的中间表 flow={} table={}", flowId, table);
            } catch (RuntimeException e) {
                log.warn("保存画布清理中间表失败 flow={} table={}：{}", flowId, table, e.getMessage());
                continue;
            }
            try {
                tableRegistryMapper.delete(new LambdaQueryWrapper<TableRegistryEntity>()
                        .eq(TableRegistryEntity::getFlowId, flowId)
                        .eq(TableRegistryEntity::getKind, "MID")
                        .eq(TableRegistryEntity::getDorisTable, table));
            } catch (RuntimeException e) {
                log.warn("保存画布清理 table_registry 失败 flow={} table={}：{}",
                        flowId, table, e.getMessage());
            }
        }
        // 被删节点若曾是 Sink，清理其水位线；目标表不在此删除。
        try {
            for (SinkWatermarkEntity mark : sinkWatermarkRepository.listByFlow(flowId)) {
                if (mark.getNodeId() != null && !sinkNodeIds.contains(mark.getNodeId())) {
                    sinkWatermarkRepository.deleteNodeTarget(flowId, mark.getNodeId(),
                            mark.getTargetKey());
                    log.info("保存画布清理已删除节点的 Sink 水位线 flow={} node={}",
                            flowId, mark.getNodeId());
                }
            }
        } catch (RuntimeException e) {
            log.warn("保存画布清理水位线失败 flow={}：{}", flowId, e.getMessage());
        }
    }

    /**
     * Sink 保存期校验（4007）：静态字段、watermarkColumn/字段映射对上游输出列、
     * targetType 与 dataSourceId 类型一致（D-M3.7-04）。
     */
    private void validateSinkConfigs(Long flowId, JsonNode dagJson) {
        Dag dag = Dag.parse(dagJson);
        for (DagNode node : dag.nodeList()) {
            if (!"sink".equals(node.type()) || isPlaceholderSink(node)) {
                continue;
            }
            SinkConfig config = sinkConfigParser.parse(node);
            sinkConfigParser.resolve(config, upstreamOutputColumns(dag, dagJson, flowId, node));
            validateTargetDataSource(config);
        }
    }

    /** 未配置任何 Sink 字段的占位节点允许保存（兼容历史画布，运行期再报错）。 */
    private boolean isPlaceholderSink(DagNode node) {
        JsonNode config = node.config();
        if (config == null || !config.isObject()) {
            return true;
        }
        if (StringUtils.hasText(node.text("tableName", null))) {
            return false;
        }
        String[] keys = {"targetType", "dataSourceId", "writeMode", "batchSize",
                "autoCreateTable", "watermarkColumn", "fieldMapping", "fields"};
        for (String key : keys) {
            JsonNode value = config.get(key);
            if (value != null && !value.isNull()
                    && !(value.isTextual() && value.asText().trim().isEmpty())) {
                return false;
            }
        }
        return true;
    }

    /** 上游输出列仅用于保存期语义校验；配置不完整时返回空列表，运行期再按物理表校验。 */
    private List<OutputColumn> upstreamOutputColumns(Dag dag, JsonNode dagJson, Long flowId,
                                                     DagNode sinkNode) {
        DagNode upstream = dag.firstUpstream(sinkNode.id());
        if (upstream == null) {
            return java.util.Collections.emptyList();
        }
        try {
            return sqlBuilderFactory.build(dagJson, flowId, upstream.id()).getOutputColumns();
        } catch (RuntimeException e) {
            log.warn("Sink 保存校验跳过上游输出列解析 flow={} node={}：{}",
                    flowId, sinkNode.id(), e.getMessage());
            return java.util.Collections.emptyList();
        }
    }

    private void validateTargetDataSource(SinkConfig config) {
        Long dsId = config.getDataSourceId();
        if (dsId == null) {
            throw SinkConfigParser.invalid(config.getNodeId(), "DATA_SOURCE_REQUIRED",
                    config.isDoris() ? "请选择 Doris 目标数据源（Stream Load 目标）" : "请选择目标数据源");
        }
        DataSourceEntity ds;
        try {
            ds = dataSourceService.getEntity(dsId);
        } catch (BizException e) {
            throw SinkConfigParser.invalid(config.getNodeId(), "DATA_SOURCE_NOT_FOUND",
                    "目标数据源不存在：" + dsId);
        }
        String dsType = ds.getType() == null ? "" : ds.getType().trim().toUpperCase(Locale.ROOT);
        if (!config.getTargetType().equals(dsType)) {
            throw SinkConfigParser.invalid(config.getNodeId(), "TARGET_TYPE_MISMATCH",
                    "目标类型 " + config.getTargetType() + " 与所选数据源类型 " + dsType + " 不一致");
        }
    }

    /**
     * 删除任务（M3.6 §12）：先 DROP 本 flow 的物化表并收集失败，再清理元数据。
     * dm_mid_{flowId}_* 全删；dm_src_{dsId}_{table} 仅当无其他 flow 引用时删。
     */
    @Transactional
    public FlowDeleteResult delete(Long id, String confirmName) {
        FlowEntity flow = getEntity(id);
        if (!confirmNameMatches(flow.getName(), confirmName)) {
            throw new BizException(ErrorCode.CONFIRM_NAME_MISMATCH, "任务名称不匹配，无法删除");
        }
        Long running = flowRunMapper.selectCount(new LambdaQueryWrapper<FlowRunEntity>()
                .eq(FlowRunEntity::getFlowId, id)
                .in(FlowRunEntity::getStatus, "RUNNING", "PENDING"));
        if (running != null && running > 0) {
            throw new BizException(ErrorCode.RUN_CONFLICT, "任务正在执行，不能删除");
        }

        Dag dag = null;
        try {
            dag = Dag.parse(parseDag(flow));
        } catch (RuntimeException e) {
            log.warn("删除任务时画布无法解析 flow={}，跳过 DAG 推导，按 registry 清理：{}", id, e.getMessage());
        }

        Set<String> middleTables = middleTables(id, dag);
        Set<String> sourceTables = sourceTablesToDrop(id, dag);

        FlowDeleteResult result = new FlowDeleteResult();
        dropTables(id, middleTables, result);
        dropTables(id, sourceTables, result);

        cleanMetadata(id, result);
        flowMapper.deleteById(id);
        log.info("删除任务 id={}，Doris 删除 {} 张、失败 {} 张",
                id, result.getDeletedTables().size(), result.getFailedTables().size());
        return result;
    }

    /** DELETE confirmName：trim 后精确匹配，大小写敏感（M3.7 §21.2）。 */
    static boolean confirmNameMatches(String flowName, String confirmName) {
        String typed = confirmName == null ? "" : confirmName.trim();
        return flowName != null && !typed.isEmpty() && flowName.equals(typed);
    }

    private Set<String> middleTables(Long flowId, Dag dag) {
        Set<String> tables = new LinkedHashSet<>();
        try {
            List<TableRegistryEntity> registry = tableRegistryMapper.selectList(
                    new LambdaQueryWrapper<TableRegistryEntity>()
                            .eq(TableRegistryEntity::getFlowId, flowId)
                            .eq(TableRegistryEntity::getKind, "MID"));
            for (TableRegistryEntity entity : registry) {
                if (StringUtils.hasText(entity.getDorisTable())) {
                    tables.add(entity.getDorisTable());
                }
            }
        } catch (RuntimeException e) {
            log.warn("读取 table_registry 失败 flow={}：{}", flowId, e.getMessage());
        }
        if (dag != null) {
            for (DagNode node : dag.nodeList()) {
                if (!"source".equals(node.type()) && !"sink".equals(node.type())) {
                    tables.add(nameSanitizer.middleTable(flowId, node.id()));
                }
            }
        }
        return tables;
    }

    private Set<String> sourceTablesToDrop(Long flowId, Dag dag) {
        Set<String> tables = new LinkedHashSet<>();
        if (dag == null) {
            return tables;
        }
        Set<String> referencedElsewhere = referencedSourceKeys(flowId);
        for (DagNode node : dag.nodeList()) {
            if (!"source".equals(node.type())) {
                continue;
            }
            Long dsId = node.longValue("dataSourceId");
            String table = node.text("tableName", null);
            if (dsId == null || table == null || table.trim().isEmpty()) {
                continue;
            }
            String key = sourceKey(dsId, table);
            if (!referencedElsewhere.contains(key)) {
                tables.add(nameSanitizer.sourceTable(dsId, table.trim()));
            }
        }
        return tables;
    }

    /** 其余 flow 的 Source 引用集合；任一 flow 画布解析失败时保守保留源表。 */
    private Set<String> referencedSourceKeys(Long excludedFlowId) {
        Set<String> keys = new LinkedHashSet<>();
        List<FlowEntity> flows;
        try {
            flows = flowMapper.selectList(new LambdaQueryWrapper<FlowEntity>()
                    .ne(FlowEntity::getId, excludedFlowId));
        } catch (RuntimeException e) {
            log.warn("扫描其他任务引用失败，源表按保留处理：{}", e.getMessage());
            return keys;
        }
        for (FlowEntity other : flows) {
            Dag otherDag;
            try {
                otherDag = Dag.parse(parseDag(other));
            } catch (RuntimeException e) {
                log.warn("任务 {} 画布无法解析，其 Source 引用无法判定", other.getId());
                continue;
            }
            for (DagNode node : otherDag.nodeList()) {
                if (!"source".equals(node.type())) {
                    continue;
                }
                Long dsId = node.longValue("dataSourceId");
                String table = node.text("tableName", null);
                if (dsId != null && table != null && !table.trim().isEmpty()) {
                    keys.add(sourceKey(dsId, table));
                }
            }
        }
        return keys;
    }

    private static String sourceKey(Long dsId, String table) {
        return dsId + "|" + table.trim().toLowerCase(Locale.ROOT);
    }

    private void dropTables(Long flowId, Set<String> tables, FlowDeleteResult result) {
        for (String table : tables) {
            boolean exists;
            try {
                exists = dorisClient.tableExistsStrict(table);
            } catch (RuntimeException e) {
                recordDropFailure(flowId, table, e.getMessage(), result);
                continue;
            }
            if (!exists) {
                continue;
            }
            try {
                dorisClient.dropTable(table);
                result.getDeletedTables().add(table);
            } catch (RuntimeException e) {
                recordDropFailure(flowId, table, e.getMessage(), result);
            }
        }
    }

    private void recordDropFailure(Long flowId, String table, String reason, FlowDeleteResult result) {
        String message = reason == null ? "DROP 失败" : reason;
        log.warn("删除节点表失败 flow={} table={}：{}", flowId, table, message);
        result.getFailedTables().add(new FlowDeleteResult.FailedTable(table, message));
        try {
            SyncLogEntity entity = new SyncLogEntity();
            entity.setDorisTable(table);
            entity.setLabel("drop_flow_" + flowId);
            entity.setAttempt(1);
            entity.setStatus("FAILED");
            entity.setRowCount(0L);
            entity.setErrMsg(message.length() > 1000 ? message.substring(0, 1000) : message);
            entity.setCostMs(0L);
            entity.setCreateTime(LocalDateTime.now());
            syncLogMapper.insert(entity);
        } catch (RuntimeException e) {
            log.warn("记录 DROP 失败日志失败 table={}：{}", table, e.getMessage());
        }
    }

    private void cleanMetadata(Long flowId, FlowDeleteResult result) {
        try {
            jdbcTemplate.update("DELETE FROM data_model_node_field WHERE flow_id = ?", flowId);
        } catch (RuntimeException e) {
            log.warn("清理 node_field 失败 flow={}：{}", flowId, e.getMessage());
        }
        List<Long> runIds = new ArrayList<>();
        try {
            for (FlowRunEntity run : flowRunMapper.selectList(
                    new LambdaQueryWrapper<FlowRunEntity>().eq(FlowRunEntity::getFlowId, flowId))) {
                runIds.add(run.getId());
            }
        } catch (RuntimeException e) {
            log.warn("读取执行记录失败 flow={}：{}", flowId, e.getMessage());
        }
        try {
            nodeRunMapper.delete(new LambdaQueryWrapper<NodeRunEntity>()
                    .eq(NodeRunEntity::getFlowId, flowId));
        } catch (RuntimeException e) {
            log.warn("清理 node_run 失败 flow={}：{}", flowId, e.getMessage());
        }
        if (!runIds.isEmpty()) {
            try {
                runLogMapper.delete(new LambdaQueryWrapper<RunLogEntity>()
                        .in(RunLogEntity::getFlowRunId, runIds));
            } catch (RuntimeException e) {
                log.warn("清理 run_log 失败 flow={}：{}", flowId, e.getMessage());
            }
        }
        try {
            flowRunMapper.delete(new LambdaQueryWrapper<FlowRunEntity>()
                    .eq(FlowRunEntity::getFlowId, flowId));
        } catch (RuntimeException e) {
            log.warn("清理 flow_run 失败 flow={}：{}", flowId, e.getMessage());
        }
        // sync_log 只清理已成功删除的表；失败表的 FAILED 日志保留给运维
        if (!result.getDeletedTables().isEmpty()) {
            try {
                syncLogMapper.delete(new LambdaQueryWrapper<SyncLogEntity>()
                        .in(SyncLogEntity::getDorisTable, result.getDeletedTables()));
            } catch (RuntimeException e) {
                log.warn("清理 sync_log 失败 flow={}：{}", flowId, e.getMessage());
            }
        }
        try {
            LambdaQueryWrapper<TableRegistryEntity> registryQuery =
                    new LambdaQueryWrapper<TableRegistryEntity>()
                            .eq(TableRegistryEntity::getFlowId, flowId)
                            .eq(TableRegistryEntity::getKind, "MID");
            if (!result.getFailedTables().isEmpty()) {
                List<String> failed = result.getFailedTables().stream()
                        .map(FlowDeleteResult.FailedTable::getDorisTable)
                        .collect(Collectors.toList());
                registryQuery.notIn(TableRegistryEntity::getDorisTable, failed);
            }
            tableRegistryMapper.delete(registryQuery);
        } catch (RuntimeException e) {
            log.warn("清理 table_registry（MID）失败 flow={}：{}", flowId, e.getMessage());
        }
        // SRC 登记行的 flow_id 为空，只能按本次实际删除的表名精确清理
        if (!result.getDeletedTables().isEmpty()) {
            try {
                tableRegistryMapper.delete(new LambdaQueryWrapper<TableRegistryEntity>()
                        .in(TableRegistryEntity::getDorisTable, result.getDeletedTables()));
            } catch (RuntimeException e) {
                log.warn("清理 table_registry（按表名）失败 flow={}：{}", flowId, e.getMessage());
            }
        }
        // A42：删除任务时清理本 flow 的水位线；Sink 目标表绝不在此删除。
        sinkWatermarkRepository.deleteFlow(flowId);
        sweepOrphanRegistry(flowId);
    }

    /** 清理指向已不存在 Doris 表的 READY 登记，避免删除后遗留孤儿。 */
    private void sweepOrphanRegistry(Long flowId) {
        List<TableRegistryEntity> rows;
        try {
            rows = tableRegistryMapper.selectList(new LambdaQueryWrapper<TableRegistryEntity>()
                    .eq(TableRegistryEntity::getStatus, "READY"));
        } catch (RuntimeException e) {
            log.warn("扫描孤儿 registry 失败 flow={}：{}", flowId, e.getMessage());
            return;
        }
        List<Long> orphanIds = new ArrayList<>();
        Set<String> orphanTables = new LinkedHashSet<>();
        for (TableRegistryEntity row : rows) {
            String table = row.getDorisTable();
            if (!StringUtils.hasText(table)) {
                continue;
            }
            try {
                if (!dorisClient.tableExistsStrict(table)) {
                    orphanIds.add(row.getId());
                    orphanTables.add(table);
                }
            } catch (RuntimeException e) {
                log.warn("孤儿 registry 检查跳过 table={}：{}", table, e.getMessage());
            }
        }
        if (orphanIds.isEmpty()) {
            return;
        }
        try {
            tableRegistryMapper.deleteBatchIds(orphanIds);
            log.info("清理孤儿 registry flow={} 共 {} 行：{}", flowId, orphanIds.size(), orphanTables);
        } catch (RuntimeException e) {
            log.warn("清理孤儿 registry 失败 flow={}：{}", flowId, e.getMessage());
            return;
        }
        try {
            syncLogMapper.delete(new LambdaQueryWrapper<SyncLogEntity>()
                    .in(SyncLogEntity::getDorisTable, orphanTables));
        } catch (RuntimeException e) {
            log.warn("清理孤儿 sync_log 失败 flow={}：{}", flowId, e.getMessage());
        }
    }

    public FlowEntity getEntity(Long id) {
        if (id == null) {
            throw new BizException(ErrorCode.PARAM_ERROR, "任务 ID 不能为空");
        }
        FlowEntity entity = flowMapper.selectById(id);
        if (entity == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "任务不存在");
        }
        return entity;
    }

    public JsonNode parseDag(FlowEntity flow) {
        String json = flow.getDagJson();
        if (!StringUtils.hasText(json)) {
            ObjectNode empty = objectMapper.createObjectNode();
            empty.putArray("nodes");
            empty.putArray("edges");
            return empty;
        }
        try {
            JsonNode node = objectMapper.readTree(json);
            return node != null && node.isObject() ? node : null;
        } catch (Exception e) {
            throw new BizException(ErrorCode.DAG_INVALID, "已保存的画布 JSON 损坏，请重新保存");
        }
    }
}
