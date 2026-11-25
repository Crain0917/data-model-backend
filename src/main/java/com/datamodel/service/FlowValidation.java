package com.datamodel.service;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagEdge;
import com.datamodel.sql.DagNode;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 保存画布与执行前的 DAG 硬校验：重复 ID、悬空连线、自环、入线数量、成环。
 * 配置不完整（Join 无 Key / GroupBy 无分组）只告警不拒绝，与 API 契约 §3.4 一致。
 */
public final class FlowValidation {

    private FlowValidation() {
    }

    public static void validate(JsonNode dagJson) {
        if (dagJson == null || !dagJson.isObject() || !dagJson.has("nodes")) {
            throw invalid(null, "NO_NODES", "dagJson 缺少 nodes");
        }
        JsonNode nodes = dagJson.get("nodes");
        if (!nodes.isArray()) {
            throw invalid(null, "NO_NODES", "dagJson.nodes 必须是数组");
        }

        Set<String> ids = new LinkedHashSet<>();
        Map<String, String> types = new LinkedHashMap<>();
        for (JsonNode n : nodes) {
            String id = n.path("id").asText("").trim();
            if (id.isEmpty()) {
                throw invalid(null, "NODE_ID_EMPTY", "节点缺少 id");
            }
            if (!ids.add(id)) {
                throw invalid(id, "DUPLICATE_ID", "节点 id 重复：" + id);
            }
            String type = DagNode.normalizeType(n.path("type").asText(""));
            if (!isKnownType(type)) {
                throw invalid(id, "NODE_TYPE_UNKNOWN", "不支持的节点类型：" + type);
            }
            types.put(id, type);
        }

        JsonNode edges = dagJson.get("edges");
        if (edges != null && !edges.isNull() && !edges.isArray()) {
            throw invalid(null, "EDGE_INVALID", "dagJson.edges 必须是数组");
        }

        Map<String, List<DagEdge>> incoming = new HashMap<>();
        Map<String, Set<String>> handles = new HashMap<>();
        Set<String> seenEdgeIds = new HashSet<>();
        if (edges != null && edges.isArray()) {
            for (JsonNode e : edges) {
                String edgeId = e.path("id").asText(null);
                if (edgeId != null && !seenEdgeIds.add(edgeId)) {
                    throw invalid(null, "DUPLICATE_EDGE", "连线 id 重复：" + edgeId);
                }
                String source = e.path("source").asText("");
                String target = e.path("target").asText("");
                String handle = e.path("targetHandle").asText("in-0");
                if (!ids.contains(source) || !ids.contains(target)) {
                    throw invalid(target.isEmpty() ? source : target, "DANGLING_EDGE",
                            "连线引用了不存在的节点：" + source + " -> " + target);
                }
                if (source.equals(target)) {
                    throw invalid(target, "SELF_LOOP", "节点不能连接自己：" + target);
                }
                DagEdge edge = new DagEdge(edgeId, source, target,
                        e.path("sourceHandle").asText("out"), handle);
                incoming.computeIfAbsent(target, k -> new ArrayList<>()).add(edge);
                handles.computeIfAbsent(target, k -> new HashSet<>()).add(handle);
            }
        }

        for (String id : ids) {
            String type = types.get(id);
            int degree = incoming.getOrDefault(id, new ArrayList<>()).size();
            if ("source".equals(type)) {
                if (degree != 0) {
                    throw invalid(id, "IN_DEGREE", "Source 节点不能有入线");
                }
            } else if ("join".equals(type)) {
                if (degree != 2 || handles.get(id).size() != 2
                        || !handles.get(id).contains("in-0") || !handles.get(id).contains("in-1")) {
                    throw invalid(id, "IN_DEGREE", "Join 节点必须且只能有 in-0、in-1 两条入线");
                }
            } else if ("union".equals(type)) {
                if (degree < 1) {
                    throw invalid(id, "IN_DEGREE", "Union 节点至少需要一条入线");
                }
            } else if (degree != 1) {
                throw invalid(id, "IN_DEGREE", "该节点必须且只能有一条入线");
            }
        }

        topologicalOrder(Dag.parse(dagJson), ids);
    }

    /** 目标节点及其全部上游的拓扑顺序；目标节点最后执行。 */
    public static List<String> executionOrder(Dag dag, String targetNodeId) {
        Set<String> subset = new LinkedHashSet<>();
        subset.add(targetNodeId);
        for (DagNode a : dag.ancestors(targetNodeId)) {
            subset.add(a.id());
        }
        return topologicalOrder(dag, subset);
    }

    public static List<String> topologicalOrder(Dag dag, Collection<String> subset) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        for (String id : subset) {
            indegree.put(id, 0);
        }
        for (String id : subset) {
            for (DagEdge e : dag.incoming(id)) {
                if (subset.contains(e.getSource())) {
                    indegree.merge(id, 1, Integer::sum);
                }
            }
        }
        Deque<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> e : indegree.entrySet()) {
            if (e.getValue() == 0) {
                queue.add(e.getKey());
            }
        }
        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            order.add(id);
            for (DagEdge e : dag.outgoing(id)) {
                if (!subset.contains(e.getTarget())) {
                    continue;
                }
                int left = indegree.merge(e.getTarget(), -1, Integer::sum);
                if (left == 0) {
                    queue.add(e.getTarget());
                }
            }
        }
        if (order.size() != subset.size()) {
            for (String id : subset) {
                if (!order.contains(id)) {
                    throw invalid(id, "CYCLE", "DAG 存在环，无法确定执行顺序");
                }
            }
            throw invalid(null, "CYCLE", "DAG 存在环，无法确定执行顺序");
        }
        return order;
    }

    private static boolean isKnownType(String type) {
        return "source".equals(type) || "filter".equals(type) || "groupby".equals(type)
                || "join".equals(type) || "union".equals(type) || "convert".equals(type)
                || "calculator".equals(type) || "formula".equals(type)
                || "replace_null".equals(type) || "sort_rows".equals(type)
                || "split_fields".equals(type) || "split_fields_to_rows".equals(type)
                || "value_mapper".equals(type) || "unique_rows".equals(type)
                || "columns_to_rows".equals(type) || "rows_to_columns".equals(type)
                || "sink".equals(type);
    }

    private static BizException invalid(String nodeId, String reason, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", nodeId);
        data.put("reason", reason);
        return new BizException(ErrorCode.DAG_INVALID, message, data);
    }
}
