package com.datamodel.sql;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 节点 SQL 唯一生成方：预览、执行、日志共用。
 * 生成的 SELECT 不含 CREATE/INSERT 包装，由执行层决定如何物化。
 */
@Component
public class SqlBuilderFactory {

    private static final int MAX_DEPTH = 24;

    private final DorisNameSanitizer nameSanitizer;

    public SqlBuilderFactory(DorisNameSanitizer nameSanitizer) {
        this.nameSanitizer = nameSanitizer;
    }

    public SqlBuildResult build(JsonNode dagJson, long flowId, String targetNodeId) {
        Dag dag = Dag.parse(dagJson);
        DagNode target = dag.node(targetNodeId);
        if (target == null) {
            throw new BizException(ErrorCode.DAG_INVALID, "节点不存在：" + targetNodeId,
                    java.util.Collections.singletonMap("nodeId", targetNodeId));
        }
        Set<String> missing = new LinkedHashSet<>();
        SqlBuildResult result = buildNode(target, dag, flowId, missing);
        result.setNodeId(target.id());
        result.setNodeType(target.type());
        result.setUpstreamMissing(new ArrayList<>(missing));
        result.setOutputColumns(outputColumns(target, dag, flowId, missing, 0));
        return result;
    }

    private SqlBuildResult buildNode(DagNode node, Dag dag, long flowId, Set<String> missing) {
        String type = node.type();
        switch (type) {
            case "source":
                return buildSource(node, dag, flowId, missing);
            case "filter":
                return buildFilter(node, dag, flowId, missing);
            case "groupby":
                return buildGroupBy(node, dag, flowId, missing);
            case "join":
                return buildJoin(node, dag, flowId, missing);
            case "union":
                return buildUnion(node, dag, flowId, missing);
            case "convert":
                return buildConvert(node, dag, flowId, missing);
            case "sink":
                return buildSink(node);
            default:
                throw new BizException(ErrorCode.DAG_INVALID, "暂不支持的节点类型：" + type,
                        java.util.Collections.singletonMap("nodeId", node.id()));
        }
    }

    private SqlBuildResult buildSource(DagNode node, Dag dag, long flowId, Set<String> missing) {
        SqlBuildResult r = new SqlBuildResult();
        String table = sourceTable(node);
        String sql = table == null
                ? "-- 数据源副本表未配置：请选择数据源与表"
                : "-- 数据源副本表 " + table;
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    private SqlBuildResult buildSink(DagNode node) {
        SqlBuildResult r = new SqlBuildResult();
        String table = node.text("tableName", null);
        String type = node.text("targetType", "DORIS").trim().toUpperCase(Locale.ROOT);
        String mode = node.text("writeMode", "APPEND").trim().toUpperCase(Locale.ROOT);
        String how = "DORIS".equals(type) ? "Stream Load（Doris）" : "JDBC 批插（" + type + "）";
        String sql = table == null || table.trim().isEmpty()
                ? "-- Sink 节点尚未配置目标表"
                : "-- Sink → " + type + ":" + table.trim() + "，writeMode=" + mode
                        + "，将执行 " + how + "；预览不写入";
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    private SqlBuildResult buildFilter(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;

        boolean passthrough = node.boolValue("passthrough", node.boolValue("keepAll", false));
        JsonNode selects = node.array("select");
        List<JsonNode> keep = new ArrayList<>();
        if (selects != null) {
            for (JsonNode s : selects) {
                if (s.path("keep").asBoolean(false)) {
                    keep.add(s);
                }
            }
        }
        String projection;
        if (passthrough || keep.isEmpty()) {
            // 上游中间表含隐藏列 __dm_rowid，不能用 a.*，按逻辑输出列显式投影。
            projection = logicalProjection(up, alias, dag, flowId, missing, q);
        } else {
            List<String> cols = new ArrayList<>();
            for (JsonNode s : keep) {
                String field = s.path("field").asText("");
                String out = s.path("alias").asText(field);
                cols.add(Identifiers.ref(alias, field, q) + " AS "
                        + Identifiers.quote(Identifiers.requireAlias(out, field), q));
            }
            projection = String.join(",\n  ", cols);
        }

        List<Condition> conditions = Condition.parseAll(node.array("conditions"));
        ConditionCompiler.Compiled where = ConditionCompiler.compile(conditions, alias, q, true);
        ConditionCompiler.Compiled displayWhere = ConditionCompiler.compile(conditions, alias, q, false);

        List<String> order = new ArrayList<>();
        if (selects != null) {
            for (JsonNode s : selects) {
                String sort = s.path("sort").asText("");
                if (!sort.trim().isEmpty()) {
                    String out = s.path("alias").asText(s.path("field").asText(""));
                    order.add(Identifiers.quote(out, q) + " " + sort.trim().toUpperCase(Locale.ROOT));
                }
            }
        }
        Long limit = node.longValue("limit");
        Long offset = node.longValue("offset");

        SqlBuildResult r = new SqlBuildResult();
        r.setSql(selectSql(node.boolValue("distinct", false), projection, upTable, alias,
                where.getExpr(), order, limit, offset));
        r.setDisplaySql(selectSql(node.boolValue("distinct", false), projection, upTable, alias,
                displayWhere.getExpr(), order, limit, offset));
        r.setParams(where.getParams());
        return r;
    }

    private String selectSql(boolean distinct, String projection, String fromTable, String alias,
                             String where, List<String> order, Long limit, Long offset) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(distinct ? "DISTINCT\n  " : "\n  ").append(projection);
        sb.append("\nFROM ").append(fromTable).append(" AS ").append(Identifiers.quote(alias, Identifiers.QuoteStyle.BACKTICK));
        if (where != null && !where.isEmpty()) {
            sb.append("\nWHERE ").append(where);
        }
        if (order != null && !order.isEmpty()) {
            sb.append("\nORDER BY ").append(String.join(", ", order));
        }
        if (limit != null && limit >= 0) {
            sb.append("\nLIMIT ").append(limit);
        }
        if (offset != null && offset > 0) {
            sb.append("\nOFFSET ").append(offset);
        }
        return sb.toString();
    }

    private SqlBuildResult buildGroupBy(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;

        JsonNode groups = node.array("groupBy");
        if (groups == null) {
            groups = node.array("groups");
        }
        JsonNode aggregates = node.array("aggregates");

        List<String> selects = new ArrayList<>();
        List<String> groupRefs = new ArrayList<>();
        if (groups != null) {
            for (JsonNode g : groups) {
                String field = g.path("field").asText("");
                String out = g.path("alias").asText(field);
                selects.add(Identifiers.ref(alias, field, q)
                        + (out.equals(field) ? "" : " AS " + Identifiers.quote(Identifiers.requireAlias(out, field), q)));
                groupRefs.add(Identifiers.ref(alias, field, q));
            }
        }
        if (aggregates != null) {
            for (JsonNode a : aggregates) {
                String fn = a.path("fn").asText(a.path("func").asText("")).trim().toUpperCase(Locale.ROOT);
                String field = a.path("field").asText("");
                String out = a.path("alias").asText("");
                if (out.isEmpty()) {
                    out = fn.toLowerCase(Locale.ROOT) + "_" + field;
                }
                String agg;
                if ("COUNT".equals(fn) && field.isEmpty()) {
                    agg = "COUNT(*)";
                } else if ("COUNT_DISTINCT".equals(fn)) {
                    agg = "COUNT(DISTINCT " + Identifiers.ref(alias, field, q) + ")";
                } else {
                    agg = fn + "(" + Identifiers.ref(alias, field, q) + ")";
                }
                selects.add(agg + " AS " + Identifiers.quote(Identifiers.requireAlias(out, out), q));
            }
        }

        boolean globalAgg = node.boolValue("globalAgg", node.boolValue("globalAggregate", false));
        List<Condition> having = Condition.parseAll(node.array("having"));
        ConditionCompiler.Compiled havingSql = ConditionCompiler.compile(having, "", q, true);
        ConditionCompiler.Compiled displayHaving = ConditionCompiler.compile(having, "", q, false);
        Long limit = node.longValue("limit");
        Long offset = node.longValue("offset");

        StringBuilder sql = new StringBuilder("SELECT\n  ");
        sql.append(selects.isEmpty()
                ? logicalProjection(up, alias, dag, flowId, missing, q)
                : String.join(",\n  ", selects));
        sql.append("\nFROM ").append(upTable).append(" AS ")
                .append(Identifiers.quote(alias, q));
        if (!globalAgg && !groupRefs.isEmpty()) {
            sql.append("\nGROUP BY ").append(String.join(", ", groupRefs));
        }
        if (!havingSql.getExpr().isEmpty()) {
            sql.append("\nHAVING ").append(havingSql.getExpr());
        }
        if (limit != null && limit >= 0) {
            sql.append("\nLIMIT ").append(limit);
        }
        if (offset != null && offset > 0) {
            sql.append("\nOFFSET ").append(offset);
        }

        StringBuilder display = new StringBuilder("SELECT\n  ");
        display.append(selects.isEmpty()
                ? logicalProjection(up, alias, dag, flowId, missing, q)
                : String.join(",\n  ", selects));
        display.append("\nFROM ").append(upTable).append(" AS ")
                .append(Identifiers.quote(alias, q));
        if (!globalAgg && !groupRefs.isEmpty()) {
            display.append("\nGROUP BY ").append(String.join(", ", groupRefs));
        }
        if (!displayHaving.getExpr().isEmpty()) {
            display.append("\nHAVING ").append(displayHaving.getExpr());
        }
        if (limit != null && limit >= 0) {
            display.append("\nLIMIT ").append(limit);
        }
        if (offset != null && offset > 0) {
            display.append("\nOFFSET ").append(offset);
        }

        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql.toString());
        r.setDisplaySql(display.toString());
        r.setParams(havingSql.getParams());
        return r;
    }

    private SqlBuildResult buildJoin(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode left = dag.upstreamByHandle(node.id(), "in-0");
        DagNode right = dag.upstreamByHandle(node.id(), "in-1");
        String leftTable = left == null ? "<missing_upstream>" : tableOf(left, dag, flowId, missing);
        String rightTable = right == null ? "<missing_upstream>" : tableOf(right, dag, flowId, missing);
        if (left == null) {
            missing.add(node.id() + ":in-0");
        }
        if (right == null) {
            missing.add(node.id() + ":in-1");
        }
        String la = Identifiers.requireAlias(node.text("leftAlias", "a"), "a");
        String ra = Identifiers.requireAlias(node.text("rightAlias", "b"), "b");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;

        String joinType = node.text("joinType", "INNER").trim().toUpperCase(Locale.ROOT);
        String joinKw;
        switch (joinType) {
            case "LEFT":
                joinKw = "LEFT JOIN";
                break;
            case "RIGHT":
                joinKw = "RIGHT JOIN";
                break;
            case "FULL":
                joinKw = "FULL OUTER JOIN";
                break;
            default:
                joinKw = "INNER JOIN";
                break;
        }

        List<String> on = new ArrayList<>();
        JsonNode keys = node.array("keys");
        if (keys != null) {
            for (JsonNode k : keys) {
                String lf = k.path("left").asText(k.path("leftField").asText(""));
                String rf = k.path("right").asText(k.path("rightField").asText(""));
                if (!lf.isEmpty() && !rf.isEmpty()) {
                    on.add(Identifiers.ref(la, lf, q) + " = " + Identifiers.ref(ra, rf, q));
                }
            }
        }
        JsonNode extra = node.array("extraOn");
        if (extra == null) {
            extra = node.array("extraConditions");
        }
        if (extra != null) {
            for (JsonNode e : extra) {
                String raw = Identifiers.requireRawExpression(e.asText(null));
                if (raw != null) {
                    on.add(raw);
                }
            }
        } else {
            String raw = node.text("extraOn", null);
            if (raw != null) {
                for (String line : raw.split("\\r?\\n")) {
                    String e = Identifiers.requireRawExpression(line);
                    if (e != null) {
                        on.add(e);
                    }
                }
            }
        }

        JsonNode projection = node.array("projection");
        if (projection == null) {
            projection = node.array("select");
        }
        List<String> cols = new ArrayList<>();
        if (projection != null) {
            for (JsonNode p : projection) {
                if (!p.path("keep").asBoolean(false)) {
                    continue;
                }
                String side = p.path("side").asText("a");
                String field = p.path("field").asText("");
                String out = p.path("alias").asText(field);
                String ref = "b".equalsIgnoreCase(side)
                        ? Identifiers.ref(ra, field, q)
                        : Identifiers.ref(la, field, q);
                cols.add(ref + " AS " + Identifiers.quote(Identifiers.requireAlias(out, field), q));
            }
        }

        StringBuilder sql = new StringBuilder("SELECT\n  ");
        sql.append(cols.isEmpty()
                ? joinProjection(left, la, right, ra, dag, flowId, missing, q)
                : String.join(",\n  ", cols));
        sql.append("\nFROM ").append(leftTable).append(" AS ").append(Identifiers.quote(la, q));
        sql.append("\n").append(joinKw).append(" ").append(rightTable).append(" AS ").append(Identifiers.quote(ra, q));
        if (!on.isEmpty()) {
            sql.append("\n  ON ").append(String.join("\n  AND ", on));
        } else {
            sql.append("\n  ON <至少一个等值 Key>");
        }

        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql.toString());
        r.setDisplaySql(sql.toString());
        return r;
    }

    private SqlBuildResult buildUnion(DagNode node, Dag dag, long flowId, Set<String> missing) {
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<DagEdge> ins = dag.incoming(node.id());
        if (ins.isEmpty()) {
            missing.add(node.id() + ":no-upstream");
        }
        String align = node.text("align", node.text("alignBy", "NAME")).trim().toUpperCase(Locale.ROOT);
        String mode = node.text("mode", node.text("unionType", "ALL")).trim().toUpperCase(Locale.ROOT);
        boolean all = !"DISTINCT".equals(mode);

        JsonNode outputs = node.array("outputs");
        if (outputs == null) {
            outputs = node.array("targets");
        }

        List<String> branches = new ArrayList<>();
        for (int i = 0; i < ins.size(); i++) {
            DagNode up = dag.node(ins.get(i).getSource());
            String table = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
            String aliasLetter = String.valueOf((char) ('a' + i));
            if ("POSITION".equals(align)) {
                branches.add("SELECT\n  " + logicalProjection(up, null, dag, flowId, missing, q)
                        + "\nFROM " + table);
                continue;
            }
            List<String> cols = new ArrayList<>();
            if (outputs != null) {
                for (JsonNode o : outputs) {
                    String name = o.path("name").asText(o.path("target").asText(""));
                    JsonNode mapping = o.has("mapping") ? o.get("mapping") : o.get("sources");
                    String src = null;
                    if (mapping != null) {
                        if (mapping.has(aliasLetter)) {
                            src = mapping.get(aliasLetter).asText(null);
                        } else if (mapping.has(ins.get(i).getSource())) {
                            src = mapping.get(ins.get(i).getSource()).asText(null);
                        } else if (mapping.has(String.valueOf(i))) {
                            src = mapping.get(String.valueOf(i)).asText(null);
                        }
                    }
                    if (name.isEmpty()) {
                        continue;
                    }
                    if (src == null || src.isEmpty()) {
                        cols.add("NULL AS " + Identifiers.quote(name, Identifiers.QuoteStyle.BACKTICK));
                    } else {
                        cols.add(Identifiers.quote(src, Identifiers.QuoteStyle.BACKTICK)
                                + " AS " + Identifiers.quote(name, Identifiers.QuoteStyle.BACKTICK));
                    }
                }
            }
            branches.add("SELECT\n  " + (cols.isEmpty()
                    ? logicalProjection(up, null, dag, flowId, missing, q)
                    : String.join(",\n  ", cols))
                    + "\nFROM " + table);
        }

        String body = String.join("\nUNION ALL\n", branches);
        String sql;
        if (all) {
            sql = body;
        } else {
            sql = "SELECT DISTINCT * FROM (\n" + body + "\n) union_distinct";
        }
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    private SqlBuildResult buildConvert(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<OutputColumn> upCols = up == null ? new ArrayList<>()
                : outputColumns(up, dag, flowId, missing, 0);
        List<ConvertRow> rows = parseConversions(node);
        validateConversions(node, rows, upCols);

        List<String> selects = new ArrayList<>();
        for (OutputColumn column : upCols) {
            ConvertRow row = matchConversion(rows, column.getName());
            if (row != null && row.remove) {
                continue;
            }
            String base = Identifiers.ref(alias, column.getName(), q);
            String expr = row == null ? base : convertExpression(base, row);
            String out = outputName(row, column.getName());
            if (expr.equals(base) && out.equals(column.getName())) {
                selects.add(base);
            } else {
                selects.add(expr + " AS " + Identifiers.quote(Identifiers.requireAlias(out, column.getName()), q));
            }
        }

        String sql = "SELECT\n  " + String.join(",\n  ", selects)
                + "\nFROM " + upTable + " AS " + Identifiers.quote(alias, q);
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    // ------------------------------------------------------- output columns

    private List<OutputColumn> outputColumns(DagNode node, Dag dag, long flowId,
                                             Set<String> missing, int depth) {
        if (depth > MAX_DEPTH) {
            return new ArrayList<>();
        }
        switch (node.type()) {
            case "source":
                return sourceColumns(node);
            case "filter": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                boolean passthrough = node.boolValue("passthrough", node.boolValue("keepAll", false));
                JsonNode selects = node.array("select");
                if (passthrough || selects == null) {
                    return upCols;
                }
                List<OutputColumn> result = new ArrayList<>();
                for (JsonNode s : selects) {
                    if (!s.path("keep").asBoolean(false)) {
                        continue;
                    }
                    String field = s.path("field").asText("");
                    String out = s.path("alias").asText(field);
                    result.add(new OutputColumn(out, typeOf(upCols, field, s.path("type").asText("STRING"))));
                }
                return result.isEmpty() ? upCols : result;
            }
            case "groupby": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                List<OutputColumn> result = new ArrayList<>();
                JsonNode groups = node.array("groupBy");
                if (groups == null) {
                    groups = node.array("groups");
                }
                if (groups != null) {
                    for (JsonNode g : groups) {
                        String field = g.path("field").asText("");
                        String out = g.path("alias").asText(field);
                        result.add(new OutputColumn(out, typeOf(upCols, field, g.path("type").asText("STRING"))));
                    }
                }
                JsonNode aggregates = node.array("aggregates");
                if (aggregates != null) {
                    for (JsonNode a : aggregates) {
                        String fn = a.path("fn").asText(a.path("func").asText("")).toUpperCase(Locale.ROOT);
                        String field = a.path("field").asText("");
                        String out = a.path("alias").asText(fn.toLowerCase(Locale.ROOT) + "_" + field);
                        String type;
                        if ("COUNT".equals(fn) || "COUNT_DISTINCT".equals(fn)) {
                            type = "BIGINT";
                        } else if ("AVG".equals(fn)) {
                            type = "DOUBLE";
                        } else if ("SUM".equals(fn)) {
                            type = numericOrDouble(typeOf(upCols, field, "DOUBLE"));
                        } else {
                            type = typeOf(upCols, field, "STRING");
                        }
                        result.add(new OutputColumn(out, type));
                    }
                }
                return result;
            }
            case "join": {
                DagNode left = dag.upstreamByHandle(node.id(), "in-0");
                DagNode right = dag.upstreamByHandle(node.id(), "in-1");
                List<OutputColumn> leftCols = left == null ? new ArrayList<>()
                        : outputColumns(left, dag, flowId, missing, depth + 1);
                List<OutputColumn> rightCols = right == null ? new ArrayList<>()
                        : outputColumns(right, dag, flowId, missing, depth + 1);
                JsonNode projection = node.array("projection");
                if (projection == null) {
                    projection = node.array("select");
                }
                List<OutputColumn> result = new ArrayList<>();
                if (projection != null) {
                    for (JsonNode p : projection) {
                        if (!p.path("keep").asBoolean(false)) {
                            continue;
                        }
                        String side = p.path("side").asText("a");
                        String field = p.path("field").asText("");
                        String out = p.path("alias").asText(field);
                        List<OutputColumn> cols = "b".equalsIgnoreCase(side) ? rightCols : leftCols;
                        result.add(new OutputColumn(out, typeOf(cols, field, p.path("type").asText("STRING"))));
                    }
                }
                return result;
            }
            case "union": {
                JsonNode outputs = node.array("outputs");
                if (outputs == null) {
                    outputs = node.array("targets");
                }
                List<DagEdge> ins = dag.incoming(node.id());
                List<OutputColumn> firstCols = ins.isEmpty() ? new ArrayList<>()
                        : outputColumns(dag.node(ins.get(0).getSource()), dag, flowId, missing, depth + 1);
                String align = node.text("align", node.text("alignBy", "NAME"))
                        .trim().toUpperCase(Locale.ROOT);
                if ("POSITION".equals(align)) {
                    // 按位置对齐：输出列取第一个分支的列名与类型。
                    return firstCols;
                }
                List<OutputColumn> result = new ArrayList<>();
                if (outputs == null) {
                    return result;
                }
                for (JsonNode o : outputs) {
                    String name = o.path("name").asText(o.path("target").asText(""));
                    JsonNode mapping = o.has("mapping") ? o.get("mapping") : o.get("sources");
                    String src = mapping == null ? null : mapping.path("a").asText(null);
                    result.add(new OutputColumn(name, typeOf(firstCols, src == null ? name : src, "STRING")));
                }
                return result;
            }
            case "convert": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                List<ConvertRow> rows = parseConversions(node);
                validateConversions(node, rows, upCols);
                List<OutputColumn> result = new ArrayList<>();
                for (OutputColumn column : upCols) {
                    ConvertRow row = matchConversion(rows, column.getName());
                    if (row != null && row.remove) {
                        continue;
                    }
                    String type = row == null || row.targetType.isEmpty()
                            ? column.getDorisType() : convertOutputType(row);
                    result.add(new OutputColumn(outputName(row, column.getName()), type));
                }
                return result;
            }
            default:
                return new ArrayList<>();
        }
    }

    private List<OutputColumn> sourceColumns(DagNode node) {
        List<OutputColumn> result = new ArrayList<>();
        JsonNode fields = node.array("fields");
        if (fields == null) {
            return result;
        }
        for (JsonNode f : fields) {
            String name = f.path("name").asText("");
            String type = f.path("type").asText("STRING");
            if (!name.isEmpty()) {
                result.add(new OutputColumn(name, type));
            }
        }
        return result;
    }

    private static String typeOf(List<OutputColumn> columns, String name, String fallback) {
        for (OutputColumn c : columns) {
            if (c.getName().equals(name)) {
                return c.getDorisType();
            }
        }
        return fallback;
    }

    private static String numericOrDouble(String type) {
        String t = type == null ? "" : type.toUpperCase(Locale.ROOT);
        if (t.startsWith("DECIMAL") || t.startsWith("DOUBLE") || t.startsWith("FLOAT")
                || t.startsWith("BIGINT") || t.startsWith("INT") || t.startsWith("SMALLINT")
                || t.startsWith("TINYINT") || t.startsWith("LARGEINT")) {
            return type;
        }
        return "DOUBLE";
    }

    // ------------------------------------------------------------ convert

    private static final Set<String> CONVERT_TYPES = new LinkedHashSet<>(java.util.Arrays.asList(
            "STRING", "INT", "BIGINT", "DOUBLE", "DECIMAL", "DATE", "DATETIME", "BOOLEAN"));

    private static final class ConvertRow {
        final String field;
        final String targetType;
        final Integer length;
        final Integer precision;
        final String format;
        final String trim;
        final boolean emptyToNull;
        final String output;
        final boolean remove;

        ConvertRow(String field, String targetType, Integer length, Integer precision, String format,
                   String trim, boolean emptyToNull, String output, boolean remove) {
            this.field = field;
            this.targetType = targetType;
            this.length = length;
            this.precision = precision;
            this.format = format;
            this.trim = trim;
            this.emptyToNull = emptyToNull;
            this.output = output;
            this.remove = remove;
        }
    }

    private List<ConvertRow> parseConversions(DagNode node) {
        List<ConvertRow> rows = new ArrayList<>();
        JsonNode conversions = node.array("conversions");
        if (conversions == null) {
            return rows;
        }
        for (JsonNode c : conversions) {
            String field = c.path("field").asText("").trim();
            if (field.isEmpty()) {
                continue;
            }
            String targetType = c.path("targetType").asText("").trim().toUpperCase(Locale.ROOT);
            String format = c.path("format").asText("").trim();
            rows.add(new ConvertRow(
                    field,
                    targetType,
                    intOrNull(c.get("length")),
                    intOrNull(c.get("precision")),
                    format.isEmpty() ? null : format,
                    c.path("trim").asText("NONE").trim().toUpperCase(Locale.ROOT),
                    c.path("emptyToNull").asBoolean(false),
                    c.path("output").asText("").trim(),
                    c.path("remove").asBoolean(false)));
        }
        return rows;
    }

    private static Integer intOrNull(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isNumber()) {
            return value.asInt();
        }
        try {
            return Integer.valueOf(value.asText().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 按 field 匹配转换行：先精确、后忽略大小写。 */
    private static ConvertRow matchConversion(List<ConvertRow> rows, String column) {
        for (ConvertRow row : rows) {
            if (row.field.equals(column)) {
                return row;
            }
        }
        for (ConvertRow row : rows) {
            if (row.field.equalsIgnoreCase(column)) {
                return row;
            }
        }
        return null;
    }

    private static OutputColumn findColumn(List<OutputColumn> columns, String name) {
        for (OutputColumn c : columns) {
            if (c.getName().equals(name)) {
                return c;
            }
        }
        for (OutputColumn c : columns) {
            if (c.getName().equalsIgnoreCase(name)) {
                return c;
            }
        }
        return null;
    }

    private static String outputName(ConvertRow row, String upstreamName) {
        return row == null || row.output.isEmpty() ? upstreamName : row.output;
    }

    private static void validateConversions(DagNode node, List<ConvertRow> rows,
                                            List<OutputColumn> upCols) {
        if (upCols.isEmpty()) {
            throw convertError(node, "CONVERT_UPSTREAM_UNKNOWN",
                    "类型转换无法确定上游字段，请先配置上游节点并获取字段");
        }
        for (ConvertRow row : rows) {
            if (findColumn(upCols, row.field) == null) {
                throw convertError(node, "CONVERT_FIELD_UNKNOWN",
                        "类型转换引用了不存在的字段：" + row.field);
            }
            if (row.remove || row.targetType.isEmpty()) {
                continue;
            }
            if (!CONVERT_TYPES.contains(row.targetType)) {
                throw convertError(node, "CONVERT_TYPE_UNSUPPORTED",
                        "不支持的目标类型：" + row.targetType);
            }
            if ("DECIMAL".equals(row.targetType)) {
                int p = decimalPrecision(row);
                int s = decimalScale(row);
                if (p < 1 || p > 38 || s < 0 || s > p) {
                    throw convertError(node, "CONVERT_TYPE_UNSUPPORTED",
                            "不支持的目标类型：DECIMAL(" + p + "," + s + ")");
                }
            }
        }
        Set<String> outputs = new LinkedHashSet<>();
        for (OutputColumn column : upCols) {
            ConvertRow row = matchConversion(rows, column.getName());
            if (row != null && row.remove) {
                continue;
            }
            String out = outputName(row, column.getName());
            if (!outputs.add(out.toLowerCase(Locale.ROOT))) {
                throw convertError(node, "CONVERT_DUPLICATE_OUTPUT",
                        "类型转换输出字段重名：" + out);
            }
        }
    }

    private static BizException convertError(DagNode node, String reason, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", node.id());
        data.put("reason", reason);
        return new BizException(ErrorCode.DAG_INVALID, message, data);
    }

    private static int decimalPrecision(ConvertRow row) {
        return row.length == null || row.length <= 0 ? 18 : row.length;
    }

    private static int decimalScale(ConvertRow row) {
        return row.precision == null || row.precision < 0 ? 0 : row.precision;
    }

    private static String decimalType(ConvertRow row) {
        return "DECIMAL(" + decimalPrecision(row) + "," + decimalScale(row) + ")";
    }

    private static String convertExpression(String base, ConvertRow row) {
        if (row.targetType.isEmpty()) {
            return base;
        }
        String x = base;
        switch (row.trim) {
            case "LEFT":
                x = "LTRIM(" + x + ")";
                break;
            case "RIGHT":
                x = "RTRIM(" + x + ")";
                break;
            case "BOTH":
                x = "TRIM(" + x + ")";
                break;
            default:
                break;
        }
        if (row.emptyToNull) {
            x = "NULLIF(" + x + ",'')";
        }
        switch (row.targetType) {
            case "STRING":
                return "CAST(" + x + " AS STRING)";
            case "INT":
                return "CAST(" + x + " AS INT)";
            case "BIGINT":
                return "CAST(" + x + " AS BIGINT)";
            case "DOUBLE":
                return "CAST(" + x + " AS DOUBLE)";
            case "DECIMAL":
                return "CAST(" + x + " AS " + decimalType(row) + ")";
            case "DATE":
                return row.format == null
                        ? "CAST(" + x + " AS DATE)"
                        : "CAST(STR_TO_DATE(" + x + "," + stringLiteral(row.format) + ") AS DATE)";
            case "DATETIME":
                return row.format == null
                        ? "CAST(" + x + " AS DATETIME)"
                        : "STR_TO_DATE(" + x + "," + stringLiteral(row.format) + ")";
            case "BOOLEAN":
                return "CAST(" + x + " AS BOOLEAN)";
            default:
                return base;
        }
    }

    private static String convertOutputType(ConvertRow row) {
        switch (row.targetType) {
            case "STRING":
                return "VARCHAR(" + (row.length == null || row.length <= 0 ? 255 : row.length) + ")";
            case "INT":
                return "INT";
            case "BIGINT":
                return "BIGINT";
            case "DOUBLE":
                return "DOUBLE";
            case "DECIMAL":
                return decimalType(row);
            case "DATE":
                return "DATE";
            case "DATETIME":
                return "DATETIME";
            case "BOOLEAN":
                return "BOOLEAN";
            default:
                return "STRING";
        }
    }

    private static String stringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    // -------------------------------------------------------- projections

    /** 显式列出上游逻辑输出列，避免把中间表的隐藏列 __dm_rowid 带进下游。 */
    private String logicalProjection(DagNode up, String alias, Dag dag, long flowId,
                                     Set<String> missing, Identifiers.QuoteStyle q) {
        if (up == null) {
            return alias == null ? "*" : Identifiers.quote(alias, q) + ".*";
        }
        List<OutputColumn> cols = outputColumns(up, dag, flowId, missing, 0);
        if (cols.isEmpty()) {
            return alias == null ? "*" : Identifiers.quote(alias, q) + ".*";
        }
        List<String> refs = new ArrayList<>();
        for (OutputColumn c : cols) {
            refs.add(Identifiers.ref(alias, c.getName(), q));
        }
        return String.join(",\n  ", refs);
    }

    private String joinProjection(DagNode left, String la, DagNode right, String ra,
                                  Dag dag, long flowId, Set<String> missing,
                                  Identifiers.QuoteStyle q) {
        if (left == null && right == null) {
            return "*";
        }
        List<String> parts = new ArrayList<>();
        if (left != null) {
            parts.add(logicalProjection(left, la, dag, flowId, missing, q));
        }
        if (right != null) {
            parts.add(logicalProjection(right, ra, dag, flowId, missing, q));
        }
        return String.join(",\n  ", parts);
    }

    // ------------------------------------------------------------- tables

    private String tableOf(DagNode node, Dag dag, long flowId, Set<String> missing) {
        if ("source".equals(node.type())) {
            String table = sourceTable(node);
            if (table == null) {
                missing.add(node.id());
                return "<missing_upstream>";
            }
            return table;
        }
        if ("sink".equals(node.type())) {
            missing.add(node.id());
            return "<missing_upstream>";
        }
        return nameSanitizer.middleTable(flowId, node.id());
    }

    private String sourceTable(DagNode node) {
        Long dsId = node.longValue("dataSourceId");
        String table = node.text("tableName", null);
        if (dsId == null || table == null || table.trim().isEmpty()) {
            return null;
        }
        return nameSanitizer.sourceTable(dsId, table.trim());
    }
}
