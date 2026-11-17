package com.datamodel.service;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.config.DorisProperties;
import com.datamodel.domain.FlowEntity;
import com.datamodel.dto.SqlPreviewRequest;
import com.datamodel.dto.SqlPreviewResponse;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.SqlBuildResult;
import com.datamodel.sql.SqlBuilderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.Set;

@Service
public class NodeSqlService {

    private static final Logger log = LoggerFactory.getLogger(NodeSqlService.class);

    private final FlowService flowService;
    private final SqlBuilderFactory sqlBuilderFactory;
    private final DorisClient dorisClient;
    private final DorisNameSanitizer nameSanitizer;
    private final DorisProperties dorisProperties;

    public NodeSqlService(FlowService flowService,
                          SqlBuilderFactory sqlBuilderFactory,
                          DorisClient dorisClient,
                          DorisNameSanitizer nameSanitizer,
                          DorisProperties dorisProperties) {
        this.flowService = flowService;
        this.sqlBuilderFactory = sqlBuilderFactory;
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
        this.dorisProperties = dorisProperties;
    }

    public SqlPreviewResponse preview(Long flowId, String nodeId, SqlPreviewRequest request) {
        String dialect = request == null || request.getDialect() == null
                ? "DORIS" : request.getDialect().trim().toUpperCase();
        if (!"DORIS".equals(dialect)) {
            throw new BizException(ErrorCode.PARAM_ERROR, "v1 仅支持 DORIS 方言");
        }
        FlowEntity flow = flowService.getEntity(flowId);
        JsonNode dagJson = request == null ? null : request.getDagJson();
        if (dagJson == null || dagJson.isNull()) {
            dagJson = flowService.parseDag(flow);
        }
        if (dagJson == null) {
            throw new BizException(ErrorCode.DAG_INVALID, "画布尚未保存");
        }

        SqlBuildResult built = sqlBuilderFactory.build(dagJson, flowId, nodeId);

        Set<String> missing = new LinkedHashSet<>(built.getUpstreamMissing());
        missing.addAll(notMaterializedUpstream(dagJson, flowId, nodeId));

        SqlPreviewResponse response = new SqlPreviewResponse();
        response.setSql(built.getSql());
        response.setParams(built.getParams());
        response.setDisplaySql(built.getDisplaySql());
        response.setDialect(dialect);
        response.setUpstreamMissing(new java.util.ArrayList<>(missing));
        DagNode target = Dag.parse(dagJson).node(nodeId);
        if (target != null && "sink".equals(target.type())) {
            response.setSink(sinkMeta(target));
        }
        return response;
    }

    private java.util.Map<String, Object> sinkMeta(DagNode node) {
        java.util.Map<String, Object> sink = new java.util.LinkedHashMap<>();
        sink.put("targetType", node.text("targetType", "DORIS").trim().toUpperCase());
        sink.put("targetTable", node.text("tableName", ""));
        sink.put("writeMode", node.text("writeMode", "APPEND").trim().toUpperCase());
        JsonNode fields = node.array("fieldMapping");
        if (fields == null) {
            fields = node.array("fields");
        }
        java.util.List<java.util.Map<String, Object>> mapping = new java.util.ArrayList<>();
        if (fields != null) {
            for (JsonNode field : fields) {
                String source = field.path("source").asText(field.path("field").asText(""));
                java.util.Map<String, Object> item = new java.util.LinkedHashMap<>();
                item.put("source", source);
                item.put("target", field.path("target").asText(source));
                item.put("type", field.path("type").asText(null));
                mapping.add(item);
            }
        }
        sink.put("fieldMapping", mapping);
        return sink;
    }

    /** 上游节点在 Doris 中是否已物化；Doris 不可用时静默跳过，不阻断预览。 */
    private Set<String> notMaterializedUpstream(JsonNode dagJson, long flowId, String nodeId) {
        Set<String> missing = new LinkedHashSet<>();
        if (!dorisProperties.isConfigured()) {
            return missing;
        }
        Dag dag;
        try {
            dag = Dag.parse(dagJson);
        } catch (RuntimeException e) {
            return missing;
        }
        Set<String> ancestors = new LinkedHashSet<>();
        for (DagNode a : dag.ancestors(nodeId)) {
            ancestors.add(a.id());
        }
        for (DagNode node : dag.nodeList()) {
            if (!ancestors.contains(node.id())) {
                continue;
            }
            try {
                if ("source".equals(node.type())) {
                    Long dsId = node.longValue("dataSourceId");
                    String table = node.text("tableName", null);
                    if (dsId != null && table != null && !table.trim().isEmpty()
                            && !dorisClient.tableExists(nameSanitizer.sourceTable(dsId, table))) {
                        missing.add(node.id());
                    }
                } else if (!"sink".equals(node.type())
                        && !dorisClient.tableExists(nameSanitizer.middleTable(flowId, node.id()))) {
                    missing.add(node.id());
                }
            } catch (RuntimeException e) {
                log.debug("检查 Doris 表是否存在失败，跳过上游物化检查：{}", e.getMessage());
                return missing;
            }
        }
        return missing;
    }
}
