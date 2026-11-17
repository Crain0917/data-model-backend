package com.datamodel.sql;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Dag {

    private final Map<String, DagNode> nodes = new LinkedHashMap<>();
    private final Map<String, List<DagEdge>> incoming = new LinkedHashMap<>();
    private final Map<String, List<DagEdge>> outgoing = new LinkedHashMap<>();

    public static Dag parse(JsonNode dagJson) {
        if (dagJson == null || !dagJson.has("nodes")) {
            throw new BizException(ErrorCode.PARAM_ERROR, "dagJson 缺少 nodes");
        }
        Dag dag = new Dag();
        for (JsonNode n : dagJson.withArray("nodes")) {
            DagNode node = DagNode.from(n);
            if (node.id() != null && !node.id().trim().isEmpty()) {
                dag.nodes.put(node.id(), node);
                dag.incoming.put(node.id(), new ArrayList<>());
                dag.outgoing.put(node.id(), new ArrayList<>());
            }
        }
        JsonNode edges = dagJson.get("edges");
        if (edges != null && edges.isArray()) {
            for (JsonNode e : edges) {
                DagEdge edge = new DagEdge(
                        e.path("id").asText(null),
                        e.path("source").asText(null),
                        e.path("target").asText(null),
                        e.path("sourceHandle").asText("out"),
                        e.path("targetHandle").asText("in-0"));
                if (edge.getSource() == null || edge.getTarget() == null) {
                    continue;
                }
                dag.incoming.computeIfAbsent(edge.getTarget(), k -> new ArrayList<>()).add(edge);
                dag.outgoing.computeIfAbsent(edge.getSource(), k -> new ArrayList<>()).add(edge);
            }
        }
        return dag;
    }

    public DagNode node(String id) {
        return nodes.get(id);
    }

    public Map<String, DagNode> nodes() {
        return Collections.unmodifiableMap(nodes);
    }

    public List<DagNode> nodeList() {
        return new ArrayList<>(nodes.values());
    }

    public List<DagEdge> incoming(String nodeId) {
        List<DagEdge> list = incoming.get(nodeId);
        if (list == null) {
            return Collections.emptyList();
        }
        List<DagEdge> copy = new ArrayList<>(list);
        copy.sort(Comparator.comparing(e -> e.getTargetHandle() == null ? "in-0" : e.getTargetHandle()));
        return copy;
    }

    public List<DagEdge> outgoing(String nodeId) {
        List<DagEdge> list = outgoing.get(nodeId);
        return list == null ? Collections.emptyList() : Collections.unmodifiableList(list);
    }

    public DagNode upstreamByHandle(String nodeId, String handle) {
        for (DagEdge e : incoming(nodeId)) {
            if (handle.equals(e.getTargetHandle())) {
                return nodes.get(e.getSource());
            }
        }
        return null;
    }

    public DagNode firstUpstream(String nodeId) {
        List<DagEdge> list = incoming(nodeId);
        return list.isEmpty() ? null : nodes.get(list.get(0).getSource());
    }

    public List<DagNode> ancestors(String nodeId) {
        List<DagNode> result = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        collectAncestors(nodeId, seen, result);
        return result;
    }

    private void collectAncestors(String nodeId, java.util.Set<String> seen, List<DagNode> result) {
        for (DagEdge e : incoming(nodeId)) {
            if (seen.add(e.getSource())) {
                DagNode up = nodes.get(e.getSource());
                if (up != null) {
                    result.add(up);
                    collectAncestors(up.id(), seen, result);
                }
            }
        }
    }
}
