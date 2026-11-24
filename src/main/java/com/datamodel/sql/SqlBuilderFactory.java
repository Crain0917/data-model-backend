package com.datamodel.sql;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.datamodel.service.doris.SourceTableNameService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
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
    private final SourceTableNameService sourceTableNameService;

    public SqlBuilderFactory(DorisNameSanitizer nameSanitizer) {
        this(nameSanitizer, null);
    }

    @Autowired
    public SqlBuilderFactory(DorisNameSanitizer nameSanitizer,
                             SourceTableNameService sourceTableNameService) {
        this.nameSanitizer = nameSanitizer;
        this.sourceTableNameService = sourceTableNameService;
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
            case "calculator":
                return buildCalculator(node, dag, flowId, missing);
            case "formula":
                return buildFormula(node, dag, flowId, missing);
            case "replace_null":
                return buildReplaceNull(node, dag, flowId, missing);
            case "sort_rows":
                return buildSortRows(node, dag, flowId, missing);
            case "split_fields":
                return buildSplitFields(node, dag, flowId, missing);
            case "split_fields_to_rows":
                return buildSplitFieldsToRows(node, dag, flowId, missing);
            case "sink":
                return buildSink(node);
            default:
                throw new BizException(ErrorCode.DAG_INVALID, "暂不支持的节点类型：" + type,
                        java.util.Collections.singletonMap("nodeId", node.id()));
        }
    }

    private SqlBuildResult buildSource(DagNode node, Dag dag, long flowId, Set<String> missing) {
        SqlBuildResult r = new SqlBuildResult();
        String table = sourceTable(node, flowId);
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

    private SqlBuildResult buildCalculator(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<OutputColumn> upCols = up == null ? new ArrayList<>()
                : outputColumns(up, dag, flowId, missing, 0);
        List<CalculatorRow> rows = parseCalculations(node);
        validateCalculations(node, rows, upCols);

        List<String> selects = new ArrayList<>();
        for (OutputColumn column : upCols) {
            selects.add(Identifiers.ref(alias, column.getName(), q));
        }
        for (CalculatorRow row : rows) {
            CalcSpec spec = CALC_SPECS.get(row.operation);
            String a = calculatorFieldOperand(alias, upCols, row.fieldA, q);
            String b = spec.arity >= 2
                    ? calculatorArgumentOperand(alias, upCols, row.argBType, row.fieldB,
                            row.literalB, q)
                    : null;
            String c = spec.arity >= 3
                    ? calculatorArgumentOperand(alias, upCols, row.argCType, row.fieldC,
                            row.literalC, q)
                    : null;
            selects.add("CAST(" + calculatorExpression(row, a, b, c) + " AS "
                    + calculatorCastType(node, row, spec) + ") AS "
                    + Identifiers.quote(row.output, q));
        }

        String sql = "SELECT\n  " + String.join(",\n  ", selects)
                + "\nFROM " + upTable + " AS " + Identifiers.quote(alias, q);
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    private SqlBuildResult buildFormula(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<OutputColumn> upCols = up == null ? new ArrayList<>()
                : outputColumns(up, dag, flowId, missing, 0);
        List<FormulaRow> rows = parseFormulas(node);
        List<String> expressions = compileFormulas(node, rows, upCols, alias, q);

        List<String> selects = new ArrayList<>();
        for (OutputColumn column : upCols) {
            selects.add(Identifiers.ref(alias, column.getName(), q));
        }
        for (int i = 0; i < rows.size(); i++) {
            FormulaRow row = rows.get(i);
            selects.add("CAST(" + expressions.get(i) + " AS " + formulaCastType(node, row) + ") AS "
                    + Identifiers.quote(row.output, q));
        }

        String sql = "SELECT\n  " + String.join(",\n  ", selects)
                + "\nFROM " + upTable + " AS " + Identifiers.quote(alias, q);
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    // ------------------------------------------------------- replace null

    private static final Set<String> REPLACE_STRATEGIES = new LinkedHashSet<>(
            java.util.Arrays.asList("VALUE", "MAX", "MIN", "AVG", "MEDIAN", "MODE"));

    private static final class ReplaceRule {
        final String field;
        final String strategy;
        final String value;
        final String output;
        final boolean treatEmptyAsNull;

        ReplaceRule(String field, String strategy, String value, String output,
                    boolean treatEmptyAsNull) {
            this.field = field;
            this.strategy = strategy;
            this.value = value;
            this.output = output;
            this.treatEmptyAsNull = treatEmptyAsNull;
        }

        boolean inPlace(String columnName) {
            return output.isEmpty() || output.equals(columnName);
        }
    }

    private static boolean isNumericType(String type) {
        String t = type == null ? "" : type.trim().toUpperCase(Locale.ROOT);
        return t.startsWith("TINYINT") || t.startsWith("SMALLINT") || t.startsWith("INT")
                || t.startsWith("INTEGER") || t.startsWith("BIGINT") || t.startsWith("LARGEINT")
                || t.startsWith("FLOAT") || t.startsWith("DOUBLE") || t.startsWith("DECIMAL")
                || t.startsWith("NUMERIC");
    }

    private static boolean isStringType(String type) {
        String t = type == null ? "" : type.trim().toUpperCase(Locale.ROOT);
        return t.startsWith("STRING") || t.startsWith("VARCHAR") || t.startsWith("CHAR")
                || t.startsWith("TEXT");
    }

    private static boolean isNumericLiteral(String value) {
        if (value == null) {
            return false;
        }
        String v = value.trim();
        if (v.isEmpty()) {
            return false;
        }
        try {
            new java.math.BigDecimal(v);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private List<ReplaceRule> parseReplaceRules(DagNode node) {
        List<ReplaceRule> rules = new ArrayList<>();
        JsonNode arr = node.array("rules");
        if (arr == null) {
            return rules;
        }
        for (JsonNode r : arr) {
            rules.add(new ReplaceRule(
                    r.path("field").asText("").trim(),
                    r.path("strategy").asText("").trim().toUpperCase(Locale.ROOT),
                    r.hasNonNull("value") ? r.get("value").asText() : null,
                    r.path("output").asText("").trim(),
                    r.path("treatEmptyAsNull").asBoolean(false)));
        }
        return rules;
    }

    private static ReplaceRule findReplaceRule(List<ReplaceRule> rules, String column) {
        for (ReplaceRule rule : rules) {
            if (rule.field.equalsIgnoreCase(column)) {
                return rule;
            }
        }
        return null;
    }

    private static String replaceOutputType(ReplaceRule rule, OutputColumn column) {
        if ("AVG".equals(rule.strategy) || "MEDIAN".equals(rule.strategy)) {
            return "DOUBLE";
        }
        return column.getDorisType();
    }

    private void validateReplaceRules(DagNode node, List<ReplaceRule> rules,
                                      List<OutputColumn> upCols) {
        if (upCols.isEmpty()) {
            throw paramError(node, "REPLACE_UPSTREAM_UNKNOWN",
                    "替换缺失值无法确定上游字段，请先配置上游节点并获取字段");
        }
        if (rules.isEmpty()) {
            throw paramError(node, "REPLACE_RULES_EMPTY",
                    "替换缺失值至少需要配置一条规则");
        }
        Set<String> fields = new LinkedHashSet<>();
        Set<String> outputs = new LinkedHashSet<>();
        for (OutputColumn column : upCols) {
            outputs.add(column.getName().toLowerCase(Locale.ROOT));
        }
        for (ReplaceRule rule : rules) {
            if (rule.field.isEmpty()) {
                throw paramError(node, "REPLACE_FIELD_MISSING",
                        "替换缺失值的字段不能为空");
            }
            if (!fields.add(rule.field.toLowerCase(Locale.ROOT))) {
                throw paramError(node, "REPLACE_RULE_DUPLICATE",
                        "同一字段只能配置一条替换规则：" + rule.field);
            }
            OutputColumn column = findColumn(upCols, rule.field);
            if (column == null) {
                throw paramError(node, "REPLACE_FIELD_UNKNOWN",
                        "替换缺失值引用了不存在的字段：" + rule.field);
            }
            if (!REPLACE_STRATEGIES.contains(rule.strategy)) {
                throw paramError(node, "REPLACE_STRATEGY_INVALID",
                        rule.strategy.isEmpty()
                                ? "替换缺失值的策略不能为空"
                                : "替换缺失值不支持的策略：" + rule.strategy);
            }
            boolean numeric = isNumericType(column.getDorisType());
            if (!"VALUE".equals(rule.strategy) && !numeric) {
                throw paramError(node, "REPLACE_STRATEGY_UNSUPPORTED",
                        "字段 " + column.getName() + " 不是数字类型，只能选择自定义值");
            }
            if ("VALUE".equals(rule.strategy)) {
                if (rule.value == null) {
                    throw paramError(node, "REPLACE_VALUE_MISSING",
                            "字段 " + column.getName() + " 的自定义替换值不能为空");
                }
                if (numeric && !isNumericLiteral(rule.value)) {
                    throw paramError(node, "REPLACE_VALUE_INVALID",
                            "字段 " + column.getName() + " 的自定义替换值必须是数字：" + rule.value);
                }
            }
            if (rule.inPlace(column.getName())) {
                continue;
            }
            String out = requireOutputName(node, rule.output, "REPLACE_OUTPUT_INVALID",
                    "替换缺失值");
            if (!outputs.add(out.toLowerCase(Locale.ROOT))) {
                throw paramError(node, "REPLACE_OUTPUT_DUPLICATE",
                        "替换缺失值输出字段重名：" + out);
            }
        }
    }

    private static String replaceExpression(ReplaceRule rule, OutputColumn column,
                                            String alias, String upTable,
                                            Identifiers.QuoteStyle q) {
        String ref = Identifiers.ref(alias, column.getName(), q);
        boolean numeric = isNumericType(column.getDorisType());
        if ("VALUE".equals(rule.strategy)) {
            String base = ref;
            if (isStringType(column.getDorisType()) && rule.treatEmptyAsNull) {
                base = "NULLIF(" + ref + ", '')";
            }
            String literal = numeric ? rule.value.trim() : stringLiteral(rule.value);
            return "COALESCE(" + base + ", " + literal + ")";
        }
        String agg;
        switch (rule.strategy) {
            case "MAX":
                agg = "SELECT MAX(" + Identifiers.quote(column.getName(), q)
                        + ") FROM " + upTable + " AS `dm_agg`";
                break;
            case "MIN":
                agg = "SELECT MIN(" + Identifiers.quote(column.getName(), q)
                        + ") FROM " + upTable + " AS `dm_agg`";
                break;
            case "AVG":
                agg = "SELECT AVG(" + Identifiers.quote(column.getName(), q)
                        + ") FROM " + upTable + " AS `dm_agg`";
                break;
            case "MEDIAN":
                agg = "SELECT PERCENTILE(" + Identifiers.quote(column.getName(), q)
                        + ", 0.5) FROM " + upTable + " AS `dm_agg`";
                break;
            case "MODE":
                agg = "SELECT " + Identifiers.quote(column.getName(), q)
                        + " FROM " + upTable + " AS `dm_agg` WHERE "
                        + Identifiers.quote(column.getName(), q)
                        + " IS NOT NULL GROUP BY " + Identifiers.quote(column.getName(), q)
                        + " ORDER BY COUNT(*) DESC, " + Identifiers.quote(column.getName(), q)
                        + " ASC LIMIT 1";
                break;
            default:
                throw new IllegalStateException("unreachable strategy " + rule.strategy);
        }
        return "CAST(COALESCE(" + ref + ", (" + agg + ")) AS "
                + replaceOutputType(rule, column) + ")";
    }

    private SqlBuildResult buildReplaceNull(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<OutputColumn> upCols = up == null ? new ArrayList<>()
                : outputColumns(up, dag, flowId, missing, 0);
        List<ReplaceRule> rules = parseReplaceRules(node);
        validateReplaceRules(node, rules, upCols);

        List<String> selects = new ArrayList<>();
        for (OutputColumn column : upCols) {
            ReplaceRule rule = findReplaceRule(rules, column.getName());
            if (rule != null && rule.inPlace(column.getName())) {
                selects.add(replaceExpression(rule, column, alias, upTable, q)
                        + " AS " + Identifiers.quote(column.getName(), q));
            } else {
                selects.add(Identifiers.ref(alias, column.getName(), q));
            }
        }
        for (ReplaceRule rule : rules) {
            OutputColumn column = findColumn(upCols, rule.field);
            if (rule.inPlace(column.getName())) {
                continue;
            }
            selects.add(replaceExpression(rule, column, alias, upTable, q)
                    + " AS " + Identifiers.quote(rule.output, q));
        }

        String sql = "SELECT\n  " + String.join(",\n  ", selects)
                + "\nFROM " + upTable + " AS " + Identifiers.quote(alias, q);
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    // ---------------------------------------------------------- sort rows

    private static final class SortKey {
        final String field;
        final String direction;
        final List<String> values;

        SortKey(String field, String direction, List<String> values) {
            this.field = field;
            this.direction = direction;
            this.values = values;
        }
    }

    private static final Set<String> SORT_FUNCTIONS = new LinkedHashSet<>(
            java.util.Arrays.asList("ROW_NUMBER", "RANK", "DENSE_RANK"));

    private List<SortKey> parseSortKeys(JsonNode arr) {
        List<SortKey> keys = new ArrayList<>();
        if (arr == null) {
            return keys;
        }
        for (JsonNode k : arr) {
            List<String> values = new ArrayList<>();
            JsonNode v = k.get("values");
            if (v != null && v.isArray()) {
                for (JsonNode item : v) {
                    values.add(item.asText(""));
                }
            }
            keys.add(new SortKey(
                    k.path("field").asText("").trim(),
                    k.path("direction").asText("").trim().toUpperCase(Locale.ROOT),
                    values));
        }
        return keys;
    }

    private static List<String> fieldNameList(JsonNode arr) {
        List<String> list = new ArrayList<>();
        if (arr == null) {
            return list;
        }
        for (JsonNode n : arr) {
            String name = n.isObject() ? n.path("field").asText("") : n.asText("");
            list.add(name.trim());
        }
        return list;
    }

    private void validateSortKeys(DagNode node, List<SortKey> keys, List<OutputColumn> upCols,
                                  String label, boolean allowCustom) {
        for (SortKey key : keys) {
            if (key.field.isEmpty()) {
                throw paramError(node, "SORT_FIELD_MISSING",
                        label + "的排序字段不能为空");
            }
            OutputColumn column = findColumn(upCols, key.field);
            if (column == null) {
                throw paramError(node, "SORT_FIELD_UNKNOWN",
                        label + "引用了不存在的字段：" + key.field);
            }
            if (!"ASC".equals(key.direction) && !"DESC".equals(key.direction)
                    && !(allowCustom && "CUSTOM".equals(key.direction))) {
                throw paramError(node, "SORT_DIRECTION_INVALID",
                        label + "不支持的排序方向：" + key.direction);
            }
            if ("CUSTOM".equals(key.direction)) {
                if (key.values.isEmpty()) {
                    throw paramError(node, "SORT_VALUES_EMPTY",
                            label + "的自定义排序值至少需要一个：" + key.field);
                }
                if (isNumericType(column.getDorisType())) {
                    for (String value : key.values) {
                        if (!isNumericLiteral(value)) {
                            throw paramError(node, "SORT_VALUE_INVALID",
                                    label + "的自定义排序值必须是数字：" + value);
                        }
                    }
                }
            }
        }
    }

    private void validateSortRows(DagNode node, List<OutputColumn> upCols,
                                  boolean simpleEnabled, List<SortKey> keys,
                                  boolean groupEnabled, List<String> partitionBy,
                                  List<SortKey> groupOrder, String groupFunction,
                                  String groupOutput, boolean sequenceEnabled,
                                  String sequenceOutput, boolean emitSequence,
                                  boolean emitGroup) {
        if (upCols.isEmpty()) {
            throw paramError(node, "SORT_UPSTREAM_UNKNOWN",
                    "排序节点无法确定上游字段，请先配置上游节点并获取字段");
        }
        Set<String> outputs = new LinkedHashSet<>();
        for (OutputColumn column : upCols) {
            outputs.add(column.getName().toLowerCase(Locale.ROOT));
        }
        if (simpleEnabled) {
            if (keys.isEmpty()) {
                throw paramError(node, "SORT_KEYS_EMPTY",
                        "简单排序至少需要一个排序字段");
            }
            validateSortKeys(node, keys, upCols, "简单排序", true);
        }
        if (groupEnabled) {
            if (partitionBy.isEmpty()) {
                throw paramError(node, "SORT_PARTITION_EMPTY",
                        "组内排序至少需要一个分区字段");
            }
            for (String field : partitionBy) {
                if (field.isEmpty()) {
                    throw paramError(node, "SORT_PARTITION_EMPTY",
                            "组内排序的分区字段不能为空");
                }
                if (findColumn(upCols, field) == null) {
                    throw paramError(node, "SORT_FIELD_UNKNOWN",
                            "组内排序引用了不存在的分区字段：" + field);
                }
            }
            if (groupOrder.isEmpty()) {
                throw paramError(node, "SORT_GROUP_ORDER_EMPTY",
                        "组内排序至少需要一个排序字段");
            }
            validateSortKeys(node, groupOrder, upCols, "组内排序", false);
            if (!SORT_FUNCTIONS.contains(groupFunction)) {
                throw paramError(node, "SORT_FUNCTION_INVALID",
                        groupFunction.isEmpty()
                                ? "组内排序的窗口函数不能为空"
                                : "组内排序不支持的窗口函数：" + groupFunction);
            }
            if (groupOutput.isEmpty()) {
                throw paramError(node, "SORT_GROUP_OUTPUT_EMPTY",
                        "组内排序的名次列名不能为空");
            }
            String out = requireOutputName(node, groupOutput, "SORT_OUTPUT_INVALID", "排序");
            if (!outputs.add(out.toLowerCase(Locale.ROOT))) {
                throw paramError(node, "SORT_OUTPUT_DUPLICATE",
                        "排序输出字段重名：" + out);
            }
        }
        if (emitSequence) {
            if (sequenceOutput.isEmpty()) {
                throw paramError(node, "SORT_SEQUENCE_OUTPUT_EMPTY",
                        "启用顺序列时必须填写顺序列名");
            }
            String out = requireOutputName(node, sequenceOutput, "SORT_OUTPUT_INVALID", "排序");
            if (!outputs.add(out.toLowerCase(Locale.ROOT))) {
                throw paramError(node, "SORT_OUTPUT_DUPLICATE",
                        "排序输出字段重名：" + out);
            }
        }
    }

    private List<String> finalSortOrder(String alias, List<OutputColumn> upCols,
                                        boolean simpleEnabled, List<SortKey> keys,
                                        boolean groupEnabled, List<String> partitionBy,
                                        List<SortKey> groupOrder, Identifiers.QuoteStyle q) {
        List<String> order = new ArrayList<>();
        if (simpleEnabled && !keys.isEmpty()) {
            for (SortKey key : keys) {
                OutputColumn column = findColumn(upCols, key.field);
                String ref = Identifiers.ref(alias, column.getName(), q);
                if ("CUSTOM".equals(key.direction)) {
                    List<String> literals = new ArrayList<>();
                    boolean numeric = isNumericType(column.getDorisType());
                    for (String value : key.values) {
                        literals.add(numeric ? value.trim() : stringLiteral(value));
                    }
                    String fieldExpr = "FIELD(" + ref + ", " + String.join(", ", literals) + ")";
                    order.add("(CASE WHEN " + ref + " IS NULL OR " + fieldExpr
                            + " = 0 THEN 1 ELSE 0 END)");
                    order.add(fieldExpr + " ASC");
                } else {
                    order.add(ref + " " + key.direction);
                }
            }
            return order;
        }
        if (groupEnabled) {
            for (String field : partitionBy) {
                order.add(Identifiers.ref(alias, findColumn(upCols, field).getName(), q) + " ASC");
            }
            for (SortKey key : groupOrder) {
                order.add(Identifiers.ref(alias, findColumn(upCols, key.field).getName(), q)
                        + " " + key.direction);
            }
        }
        return order;
    }

    private SqlBuildResult buildSortRows(DagNode node, Dag dag, long flowId, Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<OutputColumn> upCols = up == null ? new ArrayList<>()
                : outputColumns(up, dag, flowId, missing, 0);

        boolean sequenceEnabled = node.boolValue("sequenceEnabled", false);
        String sequenceOutput = node.text("sequenceOutput", "").trim();
        boolean simpleEnabled = node.boolValue("simpleEnabled", false);
        List<SortKey> keys = parseSortKeys(node.array("keys"));
        boolean groupEnabled = node.boolValue("groupEnabled", false);
        List<String> partitionBy = fieldNameList(node.array("partitionBy"));
        List<SortKey> groupOrder = parseSortKeys(node.array("groupOrderBy"));
        String groupFunction = node.text("groupFunction", "").trim().toUpperCase(Locale.ROOT);
        String groupOutput = node.text("groupOutput", "").trim();

        boolean hasFinalOrder = (simpleEnabled && !keys.isEmpty()) || groupEnabled;
        boolean emitSequence = sequenceEnabled && hasFinalOrder;
        boolean emitGroup = groupEnabled;
        validateSortRows(node, upCols, simpleEnabled, keys, groupEnabled, partitionBy,
                groupOrder, groupFunction, groupOutput, sequenceEnabled, sequenceOutput,
                emitSequence, emitGroup);
        List<String> order = finalSortOrder(alias, upCols, simpleEnabled, keys, groupEnabled,
                partitionBy, groupOrder, q);

        List<String> selects = new ArrayList<>();
        for (OutputColumn column : upCols) {
            selects.add(Identifiers.ref(alias, column.getName(), q));
        }
        if (emitSequence) {
            selects.add("ROW_NUMBER() OVER (ORDER BY " + String.join(", ", order) + ") AS "
                    + Identifiers.quote(sequenceOutput, q));
        }
        if (emitGroup) {
            List<String> partitionRefs = new ArrayList<>();
            for (String field : partitionBy) {
                partitionRefs.add(Identifiers.ref(alias, findColumn(upCols, field).getName(), q));
            }
            List<String> groupRefs = new ArrayList<>();
            for (SortKey key : groupOrder) {
                groupRefs.add(Identifiers.ref(alias, findColumn(upCols, key.field).getName(), q)
                        + " " + key.direction);
            }
            selects.add(groupFunction + "() OVER (PARTITION BY "
                    + String.join(", ", partitionRefs) + " ORDER BY "
                    + String.join(", ", groupRefs) + ") AS "
                    + Identifiers.quote(groupOutput, q));
        }

        StringBuilder sql = new StringBuilder("SELECT\n  ")
                .append(String.join(",\n  ", selects))
                .append("\nFROM ").append(upTable).append(" AS ")
                .append(Identifiers.quote(alias, q));
        if (!order.isEmpty()) {
            sql.append("\nORDER BY ").append(String.join(", ", order));
        }
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql.toString());
        r.setDisplaySql(sql.toString());
        return r;
    }

    // ------------------------------------------------------- split fields

    private static final class SplitPart {
        final Integer index;
        final String output;

        SplitPart(Integer index, String output) {
            this.index = index;
            this.output = output;
        }
    }

    private List<SplitPart> parseSplitParts(DagNode node) {
        List<SplitPart> parts = new ArrayList<>();
        JsonNode arr = node.array("parts");
        if (arr == null) {
            return parts;
        }
        for (JsonNode p : arr) {
            Integer index = intOrNull(p.get("index"));
            parts.add(new SplitPart(index, p.path("output").asText("").trim()));
        }
        return parts;
    }

    private void validateSplitFields(DagNode node, List<OutputColumn> upCols,
                                     OutputColumn sourceColumn, String delimiter,
                                     List<SplitPart> parts) {
        if (upCols.isEmpty()) {
            throw paramError(node, "SPLIT_UPSTREAM_UNKNOWN",
                    "拆分字段无法确定上游字段，请先配置上游节点并获取字段");
        }
        if (sourceColumn == null) {
            throw paramError(node, "SPLIT_FIELD_UNKNOWN",
                    "拆分字段引用了不存在的字段：" + node.text("field", "").trim());
        }
        if (delimiter == null || delimiter.isEmpty()) {
            throw paramError(node, "SPLIT_DELIMITER_EMPTY", "拆分字段的分隔符不能为空");
        }
        if (parts.isEmpty()) {
            throw paramError(node, "SPLIT_PARTS_EMPTY", "拆分字段至少需要配置一个输出片段");
        }
        Set<String> outputs = new LinkedHashSet<>();
        for (OutputColumn column : upCols) {
            outputs.add(column.getName().toLowerCase(Locale.ROOT));
        }
        Set<Integer> indexes = new LinkedHashSet<>();
        for (SplitPart part : parts) {
            if (part.index == null || part.index < 1) {
                throw paramError(node, "SPLIT_INDEX_INVALID",
                        "拆分片段的序号必须从 1 开始：" + part.index);
            }
            if (!indexes.add(part.index)) {
                throw paramError(node, "SPLIT_INDEX_DUPLICATE",
                        "拆分片段的序号不能重复：" + part.index);
            }
            if (part.output.isEmpty()) {
                throw paramError(node, "SPLIT_OUTPUT_EMPTY", "拆分片段的输出字段名不能为空");
            }
            String out = requireOutputName(node, part.output, "SPLIT_OUTPUT_INVALID", "拆分字段");
            if (!outputs.add(out.toLowerCase(Locale.ROOT))) {
                throw paramError(node, "SPLIT_OUTPUT_DUPLICATE",
                        "拆分字段输出字段重名：" + out);
            }
        }
    }

    private static String splitPartExpression(String ref, String delimiter, SplitPart part,
                                              boolean trim, boolean emptyAsNull) {
        String delimiterLiteral = stringLiteral(delimiter);
        String base;
        if (part.index == 1) {
            // SPLIT_PART 在值不含分隔符时返回 NULL；第 1 段按 Hop 语义回退为原值
            base = "(CASE WHEN LOCATE(" + delimiterLiteral + ", " + ref + ") = 0 THEN " + ref
                    + " ELSE SPLIT_PART(" + ref + ", " + delimiterLiteral + ", 1) END)";
        } else {
            base = "SPLIT_PART(" + ref + ", " + delimiterLiteral + ", " + part.index + ")";
        }
        if (trim) {
            base = "TRIM(" + base + ")";
        }
        if (emptyAsNull) {
            return "NULLIF(" + base + ", '')";
        }
        return "CASE WHEN " + ref + " IS NULL THEN NULL ELSE IFNULL(" + base + ", '') END";
    }

    private SqlBuildResult buildSplitFields(DagNode node, Dag dag, long flowId,
                                            Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<OutputColumn> upCols = up == null ? new ArrayList<>()
                : outputColumns(up, dag, flowId, missing, 0);
        String field = node.text("field", "").trim();
        OutputColumn sourceColumn = findColumn(upCols, field);
        String delimiter = node.text("delimiter", null);
        List<SplitPart> parts = parseSplitParts(node);
        validateSplitFields(node, upCols, sourceColumn, delimiter, parts);

        boolean keepOriginal = node.boolValue("keepOriginal", true);
        boolean trim = node.boolValue("trim", true);
        boolean emptyAsNull = node.boolValue("emptyAsNull", true);
        String ref = Identifiers.ref(alias, sourceColumn.getName(), q);

        List<String> selects = new ArrayList<>();
        for (OutputColumn column : upCols) {
            if (column.getName().equals(sourceColumn.getName()) && !keepOriginal) {
                continue;
            }
            selects.add(Identifiers.ref(alias, column.getName(), q));
        }
        for (SplitPart part : parts) {
            selects.add(splitPartExpression(ref, delimiter, part, trim, emptyAsNull)
                    + " AS " + Identifiers.quote(part.output, q));
        }

        String sql = "SELECT\n  " + String.join(",\n  ", selects)
                + "\nFROM " + upTable + " AS " + Identifiers.quote(alias, q);
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql);
        r.setDisplaySql(sql);
        return r;
    }

    // -------------------------------------------------- split fields to rows

    private static String uniqueAlias(List<OutputColumn> upCols, String base, Set<String> taken) {
        String name = base;
        int suffix = 2;
        while (containsColumn(upCols, name) || taken.contains(name.toLowerCase(Locale.ROOT))) {
            name = base + "_" + suffix++;
        }
        taken.add(name.toLowerCase(Locale.ROOT));
        return name;
    }

    private static boolean containsColumn(List<OutputColumn> upCols, String name) {
        for (OutputColumn column : upCols) {
            if (column.getName().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private void validateSplitRows(DagNode node, List<OutputColumn> upCols,
                                   OutputColumn sourceColumn, String delimiter, String output,
                                   boolean keepOriginal, String positionOutput) {
        if (upCols.isEmpty()) {
            throw paramError(node, "SPLIT_UPSTREAM_UNKNOWN",
                    "拆分字段为多行无法确定上游字段，请先配置上游节点并获取字段");
        }
        if (sourceColumn == null) {
            throw paramError(node, "SPLIT_FIELD_UNKNOWN",
                    "拆分字段为多行引用了不存在的字段：" + node.text("field", "").trim());
        }
        if (delimiter == null || delimiter.isEmpty()) {
            throw paramError(node, "SPLIT_DELIMITER_EMPTY", "拆分字段的分隔符不能为空");
        }
        if (output.isEmpty()) {
            throw paramError(node, "SPLIT_OUTPUT_EMPTY", "拆分字段为多行的输出字段名不能为空");
        }
        String out = requireOutputName(node, output, "SPLIT_OUTPUT_INVALID", "拆分字段为多行");
        boolean inPlace = out.equals(sourceColumn.getName());
        Set<String> outputs = new LinkedHashSet<>();
        for (OutputColumn column : upCols) {
            if (inPlace) {
                continue;
            }
            if (column.getName().equals(sourceColumn.getName()) && !keepOriginal) {
                continue;
            }
            outputs.add(column.getName().toLowerCase(Locale.ROOT));
        }
        if (!inPlace && !outputs.add(out.toLowerCase(Locale.ROOT))) {
            throw paramError(node, "SPLIT_OUTPUT_DUPLICATE",
                    "拆分字段为多行输出字段重名：" + out);
        }
        if (inPlace) {
            outputs.add(out.toLowerCase(Locale.ROOT));
        }
        if (positionOutput != null && !positionOutput.isEmpty()) {
            String pos = requireOutputName(node, positionOutput, "SPLIT_OUTPUT_INVALID",
                    "拆分字段为多行序号列");
            if (!outputs.add(pos.toLowerCase(Locale.ROOT))) {
                throw paramError(node, "SPLIT_OUTPUT_DUPLICATE",
                        "拆分字段为多行序号列重名：" + pos);
            }
        }
    }

    private SqlBuildResult buildSplitFieldsToRows(DagNode node, Dag dag, long flowId,
                                                  Set<String> missing) {
        DagNode up = dag.firstUpstream(node.id());
        String upTable = up == null ? "<missing_upstream>" : tableOf(up, dag, flowId, missing);
        if (up == null) {
            missing.add(node.id() + ":no-upstream");
        }
        String alias = Identifiers.requireAlias(node.text("alias", "a"), "a");
        Identifiers.QuoteStyle q = Identifiers.QuoteStyle.BACKTICK;
        List<OutputColumn> upCols = up == null ? new ArrayList<>()
                : outputColumns(up, dag, flowId, missing, 0);
        String field = node.text("field", "").trim();
        OutputColumn sourceColumn = findColumn(upCols, field);
        String delimiter = node.text("delimiter", null);
        String output = node.text("output", "").trim();
        boolean keepOriginal = node.boolValue("keepOriginal", true);
        boolean trim = node.boolValue("trim", true);
        boolean dropEmpty = node.boolValue("dropEmpty", false);
        String positionOutput = node.text("positionOutput", "").trim();
        validateSplitRows(node, upCols, sourceColumn, delimiter, output, keepOriginal,
                positionOutput);

        boolean inPlace = output.equals(sourceColumn.getName());
        Set<String> taken = new LinkedHashSet<>();
        String lvAlias = uniqueAlias(upCols, "__dm_lv", taken);
        String posAlias = uniqueAlias(upCols, "__dm_pos", taken);
        String valAlias = uniqueAlias(upCols, "__dm_val", taken);

        String ref = Identifiers.ref(alias, sourceColumn.getName(), q);
        String arrayExpr = "IF(" + ref + " IS NULL OR " + ref + " = '', ARRAY(NULL), "
                + "SPLIT_BY_STRING(" + ref + ", " + stringLiteral(delimiter) + "))";
        String valueExpr = trim
                ? "TRIM(" + Identifiers.quote(valAlias, q) + ")"
                : Identifiers.quote(valAlias, q);

        List<String> selects = new ArrayList<>();
        for (OutputColumn column : upCols) {
            if (column.getName().equals(sourceColumn.getName())) {
                if (inPlace) {
                    selects.add(valueExpr + " AS " + Identifiers.quote(output, q));
                } else if (keepOriginal) {
                    selects.add(Identifiers.ref(alias, column.getName(), q));
                }
                continue;
            }
            selects.add(Identifiers.ref(alias, column.getName(), q));
        }
        if (!inPlace) {
            selects.add(valueExpr + " AS " + Identifiers.quote(output, q));
        }
        if (!positionOutput.isEmpty()) {
            selects.add("(" + Identifiers.quote(posAlias, q) + " + 1) AS "
                    + Identifiers.quote(positionOutput, q));
        }

        StringBuilder sql = new StringBuilder("SELECT\n  ")
                .append(String.join(",\n  ", selects))
                .append("\nFROM ").append(upTable).append(" AS ")
                .append(Identifiers.quote(alias, q))
                .append("\nLATERAL VIEW posexplode(").append(arrayExpr).append(") ")
                .append(Identifiers.quote(lvAlias, q)).append(" AS ")
                .append(Identifiers.quote(posAlias, q)).append(", ")
                .append(Identifiers.quote(valAlias, q));
        if (dropEmpty) {
            sql.append("\nWHERE ").append(valueExpr).append(" IS NOT NULL AND ")
                    .append(valueExpr).append(" <> ''");
        }
        SqlBuildResult r = new SqlBuildResult();
        r.setSql(sql.toString());
        r.setDisplaySql(sql.toString());
        return r;
    }

    private static String requireOutputName(DagNode node, String raw, String reason, String label) {
        try {
            return Identifiers.requireField(raw);
        } catch (BizException e) {
            throw paramError(node, reason, label + "输出字段名不合法：" + raw);
        }
    }

    private static BizException paramError(DagNode node, String reason, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", node.id());
        data.put("reason", reason);
        return new BizException(ErrorCode.PARAM_ERROR, message, data);
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
            case "calculator": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                List<CalculatorRow> rows = parseCalculations(node);
                validateCalculations(node, rows, upCols);
                List<OutputColumn> result = new ArrayList<>(upCols);
                for (CalculatorRow row : rows) {
                    CalcSpec spec = CALC_SPECS.get(row.operation);
                    result.add(new OutputColumn(row.output, calculatorType(node, row, spec)));
                }
                return result;
            }
            case "formula": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                List<FormulaRow> rows = parseFormulas(node);
                compileFormulas(node, rows, upCols, null, Identifiers.QuoteStyle.BACKTICK);
                List<OutputColumn> result = new ArrayList<>(upCols);
                for (FormulaRow row : rows) {
                    result.add(new OutputColumn(row.output, formulaType(node, row)));
                }
                return result;
            }
            case "replace_null": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                List<ReplaceRule> rules = parseReplaceRules(node);
                validateReplaceRules(node, rules, upCols);
                List<OutputColumn> result = new ArrayList<>();
                for (OutputColumn column : upCols) {
                    ReplaceRule rule = findReplaceRule(rules, column.getName());
                    String type = rule != null && rule.inPlace(column.getName())
                            ? replaceOutputType(rule, column) : column.getDorisType();
                    result.add(new OutputColumn(column.getName(), type));
                }
                for (ReplaceRule rule : rules) {
                    OutputColumn column = findColumn(upCols, rule.field);
                    if (rule.inPlace(column.getName())) {
                        continue;
                    }
                    result.add(new OutputColumn(rule.output, replaceOutputType(rule, column)));
                }
                return result;
            }
            case "sort_rows": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                boolean sequenceEnabled = node.boolValue("sequenceEnabled", false);
                String sequenceOutput = node.text("sequenceOutput", "").trim();
                boolean simpleEnabled = node.boolValue("simpleEnabled", false);
                List<SortKey> keys = parseSortKeys(node.array("keys"));
                boolean groupEnabled = node.boolValue("groupEnabled", false);
                List<String> partitionBy = fieldNameList(node.array("partitionBy"));
                List<SortKey> groupOrder = parseSortKeys(node.array("groupOrderBy"));
                String groupFunction = node.text("groupFunction", "")
                        .trim().toUpperCase(Locale.ROOT);
                String groupOutput = node.text("groupOutput", "").trim();
                boolean hasFinalOrder = (simpleEnabled && !keys.isEmpty()) || groupEnabled;
                boolean emitSequence = sequenceEnabled && hasFinalOrder;
                boolean emitGroup = groupEnabled;
                validateSortRows(node, upCols, simpleEnabled, keys, groupEnabled, partitionBy,
                        groupOrder, groupFunction, groupOutput, sequenceEnabled, sequenceOutput,
                        emitSequence, emitGroup);
                List<OutputColumn> result = new ArrayList<>(upCols);
                if (emitSequence) {
                    result.add(new OutputColumn(sequenceOutput, "BIGINT"));
                }
                if (emitGroup) {
                    result.add(new OutputColumn(groupOutput, "BIGINT"));
                }
                return result;
            }
            case "split_fields": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                String field = node.text("field", "").trim();
                OutputColumn sourceColumn = findColumn(upCols, field);
                String delimiter = node.text("delimiter", null);
                List<SplitPart> parts = parseSplitParts(node);
                validateSplitFields(node, upCols, sourceColumn, delimiter, parts);
                boolean keepOriginal = node.boolValue("keepOriginal", true);
                List<OutputColumn> result = new ArrayList<>();
                for (OutputColumn column : upCols) {
                    if (column.getName().equals(sourceColumn.getName()) && !keepOriginal) {
                        continue;
                    }
                    result.add(new OutputColumn(column.getName(), column.getDorisType()));
                }
                for (SplitPart part : parts) {
                    result.add(new OutputColumn(part.output, "STRING"));
                }
                return result;
            }
            case "split_fields_to_rows": {
                DagNode up = dag.firstUpstream(node.id());
                List<OutputColumn> upCols = up == null ? new ArrayList<>()
                        : outputColumns(up, dag, flowId, missing, depth + 1);
                String field = node.text("field", "").trim();
                OutputColumn sourceColumn = findColumn(upCols, field);
                String delimiter = node.text("delimiter", null);
                String output = node.text("output", "").trim();
                boolean keepOriginal = node.boolValue("keepOriginal", true);
                String positionOutput = node.text("positionOutput", "").trim();
                validateSplitRows(node, upCols, sourceColumn, delimiter, output,
                        keepOriginal, positionOutput);
                boolean inPlace = output.equals(sourceColumn.getName());
                List<OutputColumn> result = new ArrayList<>();
                for (OutputColumn column : upCols) {
                    if (column.getName().equals(sourceColumn.getName())) {
                        if (inPlace) {
                            result.add(new OutputColumn(output, "STRING"));
                        } else if (keepOriginal) {
                            result.add(new OutputColumn(column.getName(), column.getDorisType()));
                        }
                        continue;
                    }
                    result.add(new OutputColumn(column.getName(), column.getDorisType()));
                }
                if (!inPlace) {
                    result.add(new OutputColumn(output, "STRING"));
                }
                if (!positionOutput.isEmpty()) {
                    result.add(new OutputColumn(positionOutput, "BIGINT"));
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

    // ------------------------------------------------------ calculator

    private enum CalcCategory {
        NUMERIC, DATE_SHIFT, DATE_DIFF, DATE_EXTRACT, DATE_FORMAT, DATE_PARSE,
        STRING, LENGTH, GENERIC
    }

    private static final String ARG_FIELD = "FIELD";
    private static final String ARG_LITERAL = "LITERAL";

    private static final class CalcSpec {
        final CalcCategory category;
        final int arity;
        final boolean bOptional;
        final String bDefaultMode;
        final String cDefaultMode;
        final boolean bLiteralOnly;

        CalcSpec(CalcCategory category, int arity, boolean bOptional, String bDefaultMode,
                 String cDefaultMode, boolean bLiteralOnly) {
            this.category = category;
            this.arity = arity;
            this.bOptional = bOptional;
            this.bDefaultMode = bDefaultMode;
            this.cDefaultMode = cDefaultMode;
            this.bLiteralOnly = bLiteralOnly;
        }
    }

    /** 运算 → 类别/参数个数/常量规则。MAX 仅 64 个键，使用 LinkedHashMap 保持错误提示稳定。 */
    private static final Map<String, CalcSpec> CALC_SPECS = new LinkedHashMap<>();

    static {
        numeric2("ADD");
        numeric2("SUBTRACT");
        numeric2("MULTIPLY");
        numeric2("DIVIDE");
        numeric2("MOD");
        numeric3("ADD3", ARG_FIELD, ARG_FIELD);
        numeric3("MULTIPLY3", ARG_FIELD, ARG_FIELD);
        numeric3("AVG3", ARG_FIELD, ARG_FIELD);
        numeric2("MIN");
        numeric2("MAX");
        addSpec("ROUND", CalcCategory.NUMERIC, 2, true, ARG_FIELD, ARG_FIELD, false);
        addSpec("STDROUND", CalcCategory.NUMERIC, 2, true, ARG_FIELD, ARG_FIELD, false);
        addSpec("CEIL", CalcCategory.NUMERIC, 1, false, ARG_FIELD, ARG_FIELD, false);
        addSpec("FLOOR", CalcCategory.NUMERIC, 1, false, ARG_FIELD, ARG_FIELD, false);
        addSpec("NVL", CalcCategory.GENERIC, 2, false, ARG_FIELD, ARG_FIELD, false);

        for (String unit : new String[]{"DAYS", "MONTHS", "HOURS", "MINUTES", "SECONDS"}) {
            addSpec("DATE_ADD_" + unit, CalcCategory.DATE_SHIFT, 2, false,
                    ARG_LITERAL, ARG_LITERAL, false);
            addSpec("DATE_SUB_" + unit, CalcCategory.DATE_SHIFT, 2, false,
                    ARG_LITERAL, ARG_LITERAL, false);
        }
        addSpec("DATE_DIFF_DAYS", CalcCategory.DATE_DIFF, 2, false,
                ARG_FIELD, ARG_FIELD, false);
        addSpec("DATE_DIFF_HOURS", CalcCategory.DATE_DIFF, 2, false,
                ARG_FIELD, ARG_FIELD, false);
        addSpec("DATE_DIFF_MINUTES", CalcCategory.DATE_DIFF, 2, false,
                ARG_FIELD, ARG_FIELD, false);
        addSpec("DATE_DIFF_SECONDS", CalcCategory.DATE_DIFF, 2, false,
                ARG_FIELD, ARG_FIELD, false);

        for (String op : new String[]{"YEAR_OF_DATE", "MONTH_OF_DATE", "DAY_OF_MONTH",
                "DAY_OF_WEEK", "DAY_OF_YEAR", "WEEK_OF_YEAR", "QUARTER_OF_DATE",
                "HOUR_OF_DAY", "MINUTE_OF_HOUR", "SECOND_OF_MINUTE"}) {
            addSpec(op, CalcCategory.DATE_EXTRACT, 1, false, ARG_FIELD, ARG_FIELD, false);
        }

        addSpec("DATE_FORMAT", CalcCategory.DATE_FORMAT, 2, false,
                ARG_LITERAL, ARG_LITERAL, true);
        addSpec("DATE_PARSE", CalcCategory.DATE_PARSE, 2, false,
                ARG_LITERAL, ARG_LITERAL, true);

        for (String op : new String[]{"UPPER", "LOWER", "INITCAP", "TRIM", "LTRIM", "RTRIM"}) {
            addSpec(op, CalcCategory.STRING, 1, false, ARG_FIELD, ARG_FIELD, false);
        }
        addSpec("LENGTH", CalcCategory.LENGTH, 1, false, ARG_FIELD, ARG_FIELD, false);
        addSpec("SUBSTRING", CalcCategory.STRING, 3, false,
                ARG_LITERAL, ARG_LITERAL, false);
        addSpec("LEFT", CalcCategory.STRING, 2, false, ARG_FIELD, ARG_FIELD, false);
        addSpec("RIGHT", CalcCategory.STRING, 2, false, ARG_FIELD, ARG_FIELD, false);
        addSpec("REPLACE", CalcCategory.STRING, 3, false,
                ARG_LITERAL, ARG_LITERAL, false);
        addSpec("CONCAT2", CalcCategory.STRING, 2, false, ARG_FIELD, ARG_FIELD, false);
        addSpec("CONCAT3", CalcCategory.STRING, 3, false, ARG_FIELD, ARG_FIELD, false);
        addSpec("LPAD", CalcCategory.STRING, 3, false,
                ARG_LITERAL, ARG_LITERAL, false);
        addSpec("RPAD", CalcCategory.STRING, 3, false,
                ARG_LITERAL, ARG_LITERAL, false);
    }

    private static void numeric2(String op) {
        addSpec(op, CalcCategory.NUMERIC, 2, false, ARG_FIELD, ARG_FIELD, false);
    }

    private static void numeric3(String op, String bMode, String cMode) {
        addSpec(op, CalcCategory.NUMERIC, 3, false, bMode, cMode, false);
    }

    private static void addSpec(String op, CalcCategory category, int arity, boolean bOptional,
                                String bMode, String cMode, boolean bLiteralOnly) {
        CALC_SPECS.put(op, new CalcSpec(category, arity, bOptional, bMode, cMode, bLiteralOnly));
    }

    private static final Set<String> CALC_TYPES = new LinkedHashSet<>(java.util.Arrays.asList(
            "DOUBLE", "DECIMAL", "BIGINT", "INT", "DATE", "DATETIME", "STRING"));

    private static final class CalculatorRow {
        final String output;
        final String operation;
        final String fieldA;
        final String fieldB;
        final String fieldC;
        final String argBType;
        final String argCType;
        final String literalB;
        final String literalC;
        final String returnType;
        final Integer length;
        final Integer precision;

        CalculatorRow(String output, String operation, String fieldA, String fieldB, String fieldC,
                      String argBType, String argCType, String literalB, String literalC,
                      String returnType, Integer length, Integer precision) {
            this.output = output;
            this.operation = operation;
            this.fieldA = fieldA;
            this.fieldB = fieldB;
            this.fieldC = fieldC;
            this.argBType = argBType;
            this.argCType = argCType;
            this.literalB = literalB;
            this.literalC = literalC;
            this.returnType = returnType;
            this.length = length;
            this.precision = precision;
        }
    }

    private List<CalculatorRow> parseCalculations(DagNode node) {
        List<CalculatorRow> rows = new ArrayList<>();
        JsonNode calculations = node.array("calculations");
        if (calculations == null) {
            return rows;
        }
        for (JsonNode c : calculations) {
            String operation = c.path("operation").asText("").trim().toUpperCase(Locale.ROOT);
            CalcSpec spec = CALC_SPECS.get(operation);
            String bDefault = spec == null ? ARG_FIELD : spec.bDefaultMode;
            String cDefault = spec == null ? ARG_FIELD : spec.cDefaultMode;
            String returnType = c.path("returnType").asText("").trim().toUpperCase(Locale.ROOT);
            rows.add(new CalculatorRow(
                    c.path("output").asText("").trim(),
                    operation,
                    c.path("fieldA").asText("").trim(),
                    c.path("fieldB").asText("").trim(),
                    c.path("fieldC").asText("").trim(),
                    argumentMode(c.path("argBType").asText(""), bDefault),
                    argumentMode(c.path("argCType").asText(""), cDefault),
                    textOrNull(c.get("literalB")),
                    textOrNull(c.get("literalC")),
                    returnType,
                    intOrNull(c.get("length")),
                    intOrNull(c.get("precision"))));
        }
        return rows;
    }

    private static String argumentMode(String raw, String fallback) {
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        return raw.trim().toUpperCase(Locale.ROOT);
    }

    private static String textOrNull(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }
        return value.asText();
    }

    private static void validateCalculations(DagNode node, List<CalculatorRow> rows,
                                             List<OutputColumn> upCols) {
        Set<String> outputs = new LinkedHashSet<>();
        for (OutputColumn column : upCols) {
            outputs.add(column.getName().toLowerCase(Locale.ROOT));
        }
        for (CalculatorRow row : rows) {
            CalcSpec spec = CALC_SPECS.get(row.operation);
            if (spec == null) {
                throw nodeError(node, "CALC_OPERATION_INVALID",
                        row.operation.isEmpty()
                                ? "Calculator 运算不能为空"
                                : "Calculator 不支持的运算：" + row.operation);
            }
            requireCalculatorField(node, row, "A", row.fieldA, upCols);
            if (spec.arity >= 2) {
                validateCalculatorArgument(node, row, "B", row.argBType, row.fieldB,
                        row.literalB, spec.bLiteralOnly, spec.bOptional, upCols);
            }
            if (spec.arity >= 3) {
                validateCalculatorArgument(node, row, "C", row.argCType, row.fieldC,
                        row.literalC, false, false, upCols);
            }
            calculatorType(node, row, spec);
            String out;
            try {
                out = Identifiers.requireField(row.output);
            } catch (BizException e) {
                throw nodeError(node, "CALC_OUTPUT_INVALID",
                        row.output.isEmpty()
                                ? "Calculator 输出字段不能为空"
                                : "Calculator 输出字段名不合法：" + row.output);
            }
            if (!outputs.add(out.toLowerCase(Locale.ROOT))) {
                throw nodeError(node, "CALC_OUTPUT_DUPLICATE",
                        "Calculator 输出字段重名：" + out);
            }
        }
    }

    private static void validateCalculatorArgument(DagNode node, CalculatorRow row, String label,
                                                   String mode, String field, String literal,
                                                   boolean literalOnly, boolean optional,
                                                   List<OutputColumn> upCols) {
        boolean absent = field.isEmpty() && literal == null;
        if (optional && absent) {
            return;
        }
        if (!ARG_FIELD.equals(mode) && !ARG_LITERAL.equals(mode)) {
            throw calcArgumentError(node, row,
                    "Calculator 运算 " + row.operation + " 参数 " + label + " 的取值方式不支持：" + mode);
        }
        if (ARG_LITERAL.equals(mode)) {
            if (literal == null) {
                throw calcArgumentError(node, row,
                        "Calculator 运算 " + row.operation + " 参数 " + label + " 的常量不能为空");
            }
            if (literalOnly && literal.trim().isEmpty()) {
                throw calcArgumentError(node, row,
                        "Calculator 运算 " + row.operation + " 参数 " + label + " 需要非空的格式掩码");
            }
            return;
        }
        if (literalOnly) {
            throw calcArgumentError(node, row,
                    "Calculator 运算 " + row.operation + " 参数 " + label + " 仅支持常量模式");
        }
        requireCalculatorField(node, row, label, field, upCols);
    }

    private static BizException calcArgumentError(DagNode node, CalculatorRow row, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", node.id());
        data.put("reason", "CALC_ARGUMENT_INVALID");
        data.put("operation", row.operation);
        data.put("output", row.output);
        return new BizException(ErrorCode.DAG_INVALID, message, data);
    }

    private static void requireCalculatorField(DagNode node, CalculatorRow row, String label,
                                               String field, List<OutputColumn> upCols) {
        if (field.isEmpty()) {
            throw nodeError(node, "CALC_FIELD_MISSING",
                    "Calculator 运算 " + row.operation + " 缺少输入字段 " + label);
        }
        if (findColumn(upCols, field) == null) {
            throw nodeError(node, "CALC_FIELD_UNKNOWN",
                    "Calculator 引用了不存在的字段：" + field);
        }
    }

    private static Set<String> calculatorAllowedTypes(CalcCategory category) {
        switch (category) {
            case NUMERIC:
                return new LinkedHashSet<>(java.util.Arrays.asList(
                        "DOUBLE", "DECIMAL", "BIGINT", "INT"));
            case DATE_SHIFT:
                return new LinkedHashSet<>(java.util.Arrays.asList("DATE", "DATETIME"));
            case DATE_DIFF:
                return new LinkedHashSet<>(java.util.Arrays.asList("BIGINT", "INT"));
            case DATE_EXTRACT:
                return new LinkedHashSet<>(java.util.Arrays.asList("INT", "BIGINT"));
            case DATE_FORMAT:
            case STRING:
                return new LinkedHashSet<>(java.util.Collections.singletonList("STRING"));
            case DATE_PARSE:
                return new LinkedHashSet<>(java.util.Arrays.asList("DATETIME", "DATE"));
            case LENGTH:
                return new LinkedHashSet<>(java.util.Arrays.asList("INT", "BIGINT"));
            case GENERIC:
            default:
                return new LinkedHashSet<>(java.util.Arrays.asList(
                        "DOUBLE", "DECIMAL", "BIGINT", "INT", "STRING", "DATE", "DATETIME"));
        }
    }

    private static String calculatorDefaultType(CalcCategory category) {
        switch (category) {
            case DATE_SHIFT:
                return "DATETIME";
            case DATE_DIFF:
                return "BIGINT";
            case DATE_EXTRACT:
                return "INT";
            case DATE_FORMAT:
            case STRING:
                return "STRING";
            case DATE_PARSE:
                return "DATETIME";
            case LENGTH:
                return "BIGINT";
            case NUMERIC:
            case GENERIC:
            default:
                return "DOUBLE";
        }
    }

    private static String calculatorType(DagNode node, CalculatorRow row, CalcSpec spec) {
        String effective = row.returnType.isEmpty()
                ? calculatorDefaultType(spec.category) : row.returnType;
        if (!CALC_TYPES.contains(effective)
                || !calculatorAllowedTypes(spec.category).contains(effective)) {
            throw nodeError(node, "CALC_OUTPUT_INVALID",
                    "Calculator 运算 " + row.operation + " 不支持的目标类型：" + effective);
        }
        if ("DECIMAL".equals(effective)) {
            int p = row.length == null || row.length <= 0 ? 18 : row.length;
            int s = row.precision == null || row.precision < 0 ? 2 : row.precision;
            if (p < 1 || p > 38 || s < 0 || s > p) {
                throw nodeError(node, "CALC_OUTPUT_INVALID",
                        "Calculator 目标类型 DECIMAL(" + p + "," + s + ") 不合法");
            }
            return "DECIMAL(" + p + "," + s + ")";
        }
        if ("STRING".equals(effective)) {
            int len = row.length == null || row.length <= 0 ? 255 : row.length;
            return "VARCHAR(" + len + ")";
        }
        return effective;
    }

    private static String calculatorCastType(DagNode node, CalculatorRow row, CalcSpec spec) {
        String type = calculatorType(node, row, spec);
        return type.startsWith("VARCHAR") ? "STRING" : type;
    }

    private static String calculatorFieldOperand(String alias, List<OutputColumn> cols, String field,
                                                 Identifiers.QuoteStyle q) {
        if (field.isEmpty()) {
            return null;
        }
        OutputColumn column = findColumn(cols, field);
        return column == null ? null : Identifiers.ref(alias, column.getName(), q);
    }

    private static String calculatorArgumentOperand(String alias, List<OutputColumn> cols,
                                                    String mode, String field, String literal,
                                                    Identifiers.QuoteStyle q) {
        if (ARG_LITERAL.equals(mode)) {
            return literal == null ? null : stringLiteral(literal);
        }
        if (field.isEmpty()) {
            return null;
        }
        OutputColumn column = findColumn(cols, field);
        return column == null ? null : Identifiers.ref(alias, column.getName(), q);
    }

    private static String calculatorExpression(CalculatorRow row, String a, String b, String c) {
        switch (row.operation) {
            case "ADD":
                return a + " + " + b;
            case "SUBTRACT":
                return a + " - " + b;
            case "MULTIPLY":
                return a + " * " + b;
            case "DIVIDE":
                return a + " / " + b;
            case "MOD":
                return a + " % " + b;
            case "ADD3":
                return a + " + " + b + " + " + c;
            case "MULTIPLY3":
                return a + " * " + b + " * " + c;
            case "AVG3":
                return "(" + a + " + " + b + " + " + c + ") / 3";
            case "MIN":
                return "LEAST(" + a + ", " + b + ")";
            case "MAX":
                return "GREATEST(" + a + ", " + b + ")";
            // ROUND_BANKERS：半偶舍入（2.5→2、3.5→4、-2.5→-2），Doris 4.x 原生函数。
            case "ROUND":
                return b == null ? "ROUND_BANKERS(" + a + ")" : "ROUND_BANKERS(" + a + ", " + b + ")";
            // ROUND：半远离零（2.5→3、-2.5→-3），与 MySQL/Doris 一致。
            case "STDROUND":
                return b == null ? "ROUND(" + a + ")" : "ROUND(" + a + ", " + b + ")";
            case "CEIL":
                return "CEIL(" + a + ")";
            case "FLOOR":
                return "FLOOR(" + a + ")";
            case "NVL":
                return "IFNULL(" + a + ", " + b + ")";
            case "DATE_ADD_DAYS":
                return "DATE_ADD(" + a + ", INTERVAL " + b + " DAY)";
            case "DATE_ADD_MONTHS":
                return "DATE_ADD(" + a + ", INTERVAL " + b + " MONTH)";
            case "DATE_ADD_HOURS":
                return "DATE_ADD(" + a + ", INTERVAL " + b + " HOUR)";
            case "DATE_ADD_MINUTES":
                return "DATE_ADD(" + a + ", INTERVAL " + b + " MINUTE)";
            case "DATE_ADD_SECONDS":
                return "DATE_ADD(" + a + ", INTERVAL " + b + " SECOND)";
            case "DATE_SUB_DAYS":
                return "DATE_SUB(" + a + ", INTERVAL " + b + " DAY)";
            case "DATE_SUB_MONTHS":
                return "DATE_SUB(" + a + ", INTERVAL " + b + " MONTH)";
            case "DATE_SUB_HOURS":
                return "DATE_SUB(" + a + ", INTERVAL " + b + " HOUR)";
            case "DATE_SUB_MINUTES":
                return "DATE_SUB(" + a + ", INTERVAL " + b + " MINUTE)";
            case "DATE_SUB_SECONDS":
                return "DATE_SUB(" + a + ", INTERVAL " + b + " SECOND)";
            case "DATE_DIFF_DAYS":
                return "DATEDIFF(" + a + ", " + b + ")";
            case "DATE_DIFF_HOURS":
                return "TIMESTAMPDIFF(HOUR, " + b + ", " + a + ")";
            case "DATE_DIFF_MINUTES":
                return "TIMESTAMPDIFF(MINUTE, " + b + ", " + a + ")";
            case "DATE_DIFF_SECONDS":
                return "TIMESTAMPDIFF(SECOND, " + b + ", " + a + ")";
            case "YEAR_OF_DATE":
                return "YEAR(" + a + ")";
            case "MONTH_OF_DATE":
                return "MONTH(" + a + ")";
            case "DAY_OF_MONTH":
                return "DAY(" + a + ")";
            case "DAY_OF_WEEK":
                return "DAYOFWEEK(" + a + ")";
            case "DAY_OF_YEAR":
                return "DAYOFYEAR(" + a + ")";
            case "WEEK_OF_YEAR":
                // WEEKOFYEAR：ISO 周（周一为周首、含 4 天以上为第 1 周，范围 1-53）。
                return "WEEKOFYEAR(" + a + ")";
            case "QUARTER_OF_DATE":
                return "QUARTER(" + a + ")";
            case "HOUR_OF_DAY":
                return "HOUR(" + a + ")";
            case "MINUTE_OF_HOUR":
                return "MINUTE(" + a + ")";
            case "SECOND_OF_MINUTE":
                return "SECOND(" + a + ")";
            case "DATE_FORMAT":
                return "DATE_FORMAT(" + a + ", " + b + ")";
            case "DATE_PARSE":
                return "STR_TO_DATE(" + a + ", " + b + ")";
            case "UPPER":
                return "UPPER(" + a + ")";
            case "LOWER":
                return "LOWER(" + a + ")";
            case "INITCAP":
                return "INITCAP(" + a + ")";
            case "TRIM":
                return "TRIM(" + a + ")";
            case "LTRIM":
                return "LTRIM(" + a + ")";
            case "RTRIM":
                return "RTRIM(" + a + ")";
            case "LENGTH":
                return "LENGTH(" + a + ")";
            case "SUBSTRING":
                return "SUBSTRING(" + a + ", " + b + ", " + c + ")";
            case "LEFT":
                return "LEFT(" + a + ", " + b + ")";
            case "RIGHT":
                return "RIGHT(" + a + ", " + b + ")";
            case "REPLACE":
                return "REPLACE(" + a + ", " + b + ", " + c + ")";
            case "CONCAT2":
                return "CONCAT(" + a + ", " + b + ")";
            case "CONCAT3":
                return "CONCAT(" + a + ", " + b + ", " + c + ")";
            case "LPAD":
                return "LPAD(" + a + ", " + b + ", " + c + ")";
            case "RPAD":
                return "RPAD(" + a + ", " + b + ", " + c + ")";
            default:
                return a;
        }
    }

    // --------------------------------------------------------- formula

    private static final int MAX_FORMULA_LENGTH = 2000;

    private static final class FormulaRow {
        final String output;
        final String expression;
        final String returnType;
        final Integer length;
        final Integer precision;

        FormulaRow(String output, String expression, String returnType,
                   Integer length, Integer precision) {
            this.output = output;
            this.expression = expression;
            this.returnType = returnType;
            this.length = length;
            this.precision = precision;
        }
    }

    private List<FormulaRow> parseFormulas(DagNode node) {
        List<FormulaRow> rows = new ArrayList<>();
        JsonNode formulas = node.array("formulas");
        if (formulas == null) {
            return rows;
        }
        for (JsonNode f : formulas) {
            String returnType = f.path("returnType").asText("").trim().toUpperCase(Locale.ROOT);
            rows.add(new FormulaRow(
                    f.path("output").asText("").trim(),
                    f.path("expression").asText("").trim(),
                    returnType.isEmpty() ? "DOUBLE" : returnType,
                    intOrNull(f.get("length")),
                    intOrNull(f.get("precision"))));
        }
        return rows;
    }

    private List<String> compileFormulas(DagNode node, List<FormulaRow> rows,
                                         List<OutputColumn> upCols, String alias,
                                         Identifiers.QuoteStyle q) {
        Set<String> outputs = new LinkedHashSet<>();
        for (OutputColumn column : upCols) {
            outputs.add(column.getName().toLowerCase(Locale.ROOT));
        }
        List<String> expressions = new ArrayList<>();
        for (FormulaRow row : rows) {
            String out;
            try {
                out = Identifiers.requireField(row.output);
            } catch (BizException e) {
                throw nodeError(node, "FORMULA_OUTPUT_INVALID",
                        row.output.isEmpty()
                                ? "Formula 输出字段不能为空"
                                : "Formula 输出字段名不合法：" + row.output);
            }
            if (!outputs.add(out.toLowerCase(Locale.ROOT))) {
                throw nodeError(node, "FORMULA_OUTPUT_DUPLICATE",
                        "Formula 输出字段重名：" + out);
            }
            if (row.expression.isEmpty()) {
                throw nodeError(node, "FORMULA_EXPRESSION_EMPTY",
                        "Formula 表达式不能为空：" + out);
            }
            if (row.expression.length() > MAX_FORMULA_LENGTH) {
                throw nodeError(node, "FORMULA_SYNTAX",
                        "Formula 表达式不能超过 " + MAX_FORMULA_LENGTH + " 字符");
            }
            formulaType(node, row);
            expressions.add(new FormulaParser(node, upCols, alias, q, row.expression).parse());
        }
        return expressions;
    }

    private static String formulaType(DagNode node, FormulaRow row) {
        switch (row.returnType) {
            case "STRING":
                return "VARCHAR(" + formulaStringLength(row) + ")";
            case "DECIMAL":
                return formulaDecimalType(node, row);
            case "DOUBLE":
            case "BIGINT":
            case "INT":
                return row.returnType;
            default:
                throw nodeError(node, "FORMULA_OUTPUT_INVALID",
                        "Formula 不支持的目标类型：" + row.returnType);
        }
    }

    private static String formulaCastType(DagNode node, FormulaRow row) {
        if ("STRING".equals(row.returnType)) {
            return "STRING";
        }
        return formulaType(node, row);
    }

    private static int formulaStringLength(FormulaRow row) {
        return row.length == null || row.length <= 0 ? 255 : row.length;
    }

    private static String formulaDecimalType(DagNode node, FormulaRow row) {
        int p = row.length == null || row.length <= 0 ? 18 : row.length;
        int s = row.precision == null || row.precision < 0 ? 2 : row.precision;
        if (p < 1 || p > 38 || s < 0 || s > p) {
            throw nodeError(node, "FORMULA_OUTPUT_INVALID",
                    "Formula 目标类型 DECIMAL(" + p + "," + s + ") 不合法");
        }
        return "DECIMAL(" + p + "," + s + ")";
    }

    private static BizException nodeError(DagNode node, String reason, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", node.id());
        data.put("reason", reason);
        return new BizException(ErrorCode.DAG_INVALID, message, data);
    }

    private static final class FormulaToken {
        enum Type {
            NUMBER, STRING, FIELD, IDENT, OP, LPAREN, RPAREN, COMMA, END
        }

        final Type type;
        final String text;

        FormulaToken(Type type, String text) {
            this.type = type;
            this.text = text;
        }
    }

    private static final class FormulaExpr {
        final int precedence;
        final String sql;

        FormulaExpr(int precedence, String sql) {
            this.precedence = precedence;
            this.sql = sql;
        }
    }

    /**
     * Formula 表达式解析：只接受数字、单引号字符串、[字段]、白名单函数、+ - * / % 与括号，
     * 解析结果直接由 AST 生成 SQL，用户原文不参与拼接。
     */
    private static final class FormulaParser {

        private static final Set<String> SQL_KEYWORDS = new LinkedHashSet<>(java.util.Arrays.asList(
                "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "UNION", "JOIN", "INSERT", "UPDATE",
                "DELETE", "DROP", "CREATE", "ALTER", "INTO", "VALUES", "HAVING", "LIMIT", "WITH",
                "AND", "OR", "NOT"));

        private static final Set<String> FUNCTIONS = new LinkedHashSet<>(java.util.Arrays.asList(
                "SUM", "ABS", "ROUND", "CEIL", "CEILING", "FLOOR", "COALESCE", "IFNULL", "NULLIF",
                "CONCAT", "TRIM", "LTRIM", "RTRIM", "LOWER", "UPPER", "LENGTH", "SUBSTR",
                "SUBSTRING", "GREATEST", "LEAST",
                "NVL", "DATE_FORMAT", "STR_TO_DATE", "DATEDIFF",
                "YEAR", "MONTH", "DAY", "DAYOFWEEK", "DAYOFYEAR", "WEEK", "QUARTER",
                "HOUR", "MINUTE", "SECOND",
                "LEFT", "RIGHT", "REPLACE", "LPAD", "RPAD", "INITCAP"));

        private final DagNode node;
        private final List<OutputColumn> upCols;
        private final String alias;
        private final Identifiers.QuoteStyle q;
        private final List<FormulaToken> tokens = new ArrayList<>();
        private int pos;

        FormulaParser(DagNode node, List<OutputColumn> upCols, String alias,
                      Identifiers.QuoteStyle q, String expression) {
            this.node = node;
            this.upCols = upCols;
            this.alias = alias;
            this.q = q;
            tokenize(expression);
            tokens.add(new FormulaToken(FormulaToken.Type.END, ""));
        }

        String parse() {
            FormulaExpr expr = parseExpression();
            if (peek().type != FormulaToken.Type.END) {
                throw syntax("存在无法解析的内容：" + peek().text);
            }
            return expr.sql;
        }

        private void tokenize(String expression) {
            int i = 0;
            while (i < expression.length()) {
                char c = expression.charAt(i);
                if (Character.isWhitespace(c)) {
                    i++;
                    continue;
                }
                if ((c == '-' && i + 1 < expression.length() && expression.charAt(i + 1) == '-')
                        || (c == '/' && i + 1 < expression.length()
                        && expression.charAt(i + 1) == '*')) {
                    throw syntax("不支持注释");
                }
                if (c == '\'') {
                    StringBuilder sb = new StringBuilder();
                    i++;
                    boolean closed = false;
                    while (i < expression.length()) {
                        char s = expression.charAt(i);
                        if (s == '\'') {
                            if (i + 1 < expression.length() && expression.charAt(i + 1) == '\'') {
                                sb.append('\'');
                                i += 2;
                                continue;
                            }
                            closed = true;
                            i++;
                            break;
                        }
                        sb.append(s);
                        i++;
                    }
                    if (!closed) {
                        throw syntax("字符串未闭合");
                    }
                    tokens.add(new FormulaToken(FormulaToken.Type.STRING, sb.toString()));
                    continue;
                }
                if (Character.isDigit(c)) {
                    int start = i;
                    boolean dot = false;
                    while (i < expression.length()) {
                        char d = expression.charAt(i);
                        if (Character.isDigit(d)) {
                            i++;
                            continue;
                        }
                        if (d == '.' && !dot) {
                            dot = true;
                            i++;
                            continue;
                        }
                        break;
                    }
                    tokens.add(new FormulaToken(FormulaToken.Type.NUMBER,
                            expression.substring(start, i)));
                    continue;
                }
                if (c == '[') {
                    int end = expression.indexOf(']', i + 1);
                    if (end < 0) {
                        throw syntax("字段引用缺少 ]");
                    }
                    String name = expression.substring(i + 1, end).trim();
                    if (name.isEmpty()) {
                        throw syntax("字段引用不能为空");
                    }
                    if (name.indexOf('`') >= 0 || name.indexOf(';') >= 0 || hasControlChar(name)) {
                        throw syntax("字段引用不合法：" + name);
                    }
                    tokens.add(new FormulaToken(FormulaToken.Type.FIELD, name));
                    i = end + 1;
                    continue;
                }
                if (isIdentStart(c)) {
                    int start = i;
                    while (i < expression.length() && isIdentPart(expression.charAt(i))) {
                        i++;
                    }
                    tokens.add(new FormulaToken(FormulaToken.Type.IDENT,
                            expression.substring(start, i)));
                    continue;
                }
                switch (c) {
                    case '+':
                    case '-':
                    case '*':
                    case '/':
                    case '%':
                        tokens.add(new FormulaToken(FormulaToken.Type.OP, String.valueOf(c)));
                        i++;
                        continue;
                    case '(':
                        tokens.add(new FormulaToken(FormulaToken.Type.LPAREN, "("));
                        i++;
                        continue;
                    case ')':
                        tokens.add(new FormulaToken(FormulaToken.Type.RPAREN, ")"));
                        i++;
                        continue;
                    case ',':
                        tokens.add(new FormulaToken(FormulaToken.Type.COMMA, ","));
                        i++;
                        continue;
                    default:
                        throw syntax("包含非法字符：" + c);
                }
            }
        }

        private FormulaExpr parseExpression() {
            FormulaExpr left = parseTerm();
            while (peek().type == FormulaToken.Type.OP
                    && ("+".equals(peek().text) || "-".equals(peek().text))) {
                String op = next().text;
                left = binary(left, op, parseTerm(), 1);
            }
            return left;
        }

        private FormulaExpr parseTerm() {
            FormulaExpr left = parseUnary();
            while (peek().type == FormulaToken.Type.OP
                    && ("*".equals(peek().text) || "/".equals(peek().text)
                    || "%".equals(peek().text))) {
                String op = next().text;
                left = binary(left, op, parseUnary(), 2);
            }
            return left;
        }

        private FormulaExpr parseUnary() {
            if (peek().type == FormulaToken.Type.OP
                    && ("+".equals(peek().text) || "-".equals(peek().text))) {
                String op = next().text;
                FormulaExpr operand = parseUnary();
                String sql = operand.precedence < 3 ? "(" + operand.sql + ")" : operand.sql;
                return new FormulaExpr(3, op + sql);
            }
            return parsePrimary();
        }

        private FormulaExpr parsePrimary() {
            FormulaToken token = next();
            switch (token.type) {
                case NUMBER:
                    return new FormulaExpr(4, token.text);
                case STRING:
                    return new FormulaExpr(4, stringLiteral(token.text));
                case FIELD:
                    OutputColumn column = findColumn(upCols, token.text);
                    if (column == null) {
                        throw nodeError(node, "FORMULA_FIELD_UNKNOWN",
                                "Formula 引用了不存在的字段：" + token.text);
                    }
                    return new FormulaExpr(4, Identifiers.ref(alias, column.getName(), q));
                case IDENT:
                    if (peek().type != FormulaToken.Type.LPAREN) {
                        throw syntax("未识别的标识符：" + token.text
                                + "，字段请使用 [字段名] 引用");
                    }
                    return parseFunction(token.text);
                case LPAREN:
                    FormulaExpr inner = parseExpression();
                    expect(FormulaToken.Type.RPAREN, ")");
                    return new FormulaExpr(4, "(" + inner.sql + ")");
                default:
                    throw syntax("表达式不完整");
            }
        }

        private FormulaExpr parseFunction(String raw) {
            String name = raw.toUpperCase(Locale.ROOT);
            if (SQL_KEYWORDS.contains(name)) {
                throw syntax("表达式包含不支持的 SQL 关键字：" + name);
            }
            if (isAggregateFunction(name)) {
                throw nodeError(node, "FORMULA_AGGREGATE_UNSUPPORTED",
                        "不支持聚合函数：" + name + "（Formula 为行级变换，不支持跨行聚合）");
            }
            if (!FUNCTIONS.contains(name)) {
                throw nodeError(node, "FORMULA_FUNCTION_UNSUPPORTED",
                        "不支持的函数：" + name);
            }
            expect(FormulaToken.Type.LPAREN, "(");
            List<FormulaExpr> args = new ArrayList<>();
            if (peek().type != FormulaToken.Type.RPAREN) {
                args.add(parseExpression());
                while (peek().type == FormulaToken.Type.COMMA) {
                    next();
                    args.add(parseExpression());
                }
            }
            expect(FormulaToken.Type.RPAREN, ")");
            validateArity(name, args.size());

            if ("SUM".equals(name)) {
                List<String> parts = new ArrayList<>();
                for (FormulaExpr arg : args) {
                    parts.add(arg.precedence < 2 ? "(" + arg.sql + ")" : arg.sql);
                }
                return new FormulaExpr(4, "(" + String.join(" + ", parts) + ")");
            }
            List<String> parts = new ArrayList<>();
            for (FormulaExpr arg : args) {
                parts.add(arg.sql);
            }
            String sqlName = "NVL".equals(name) ? "IFNULL" : name;
            return new FormulaExpr(4, sqlName + "(" + String.join(", ", parts) + ")");
        }

        private static boolean isAggregateFunction(String name) {
            return "COUNT".equals(name) || "AVG".equals(name) || "MIN".equals(name)
                    || "MAX".equals(name) || "GROUP_CONCAT".equals(name)
                    || name.startsWith("STDDEV") || "VARIANCE".equals(name)
                    || name.startsWith("VAR_") || "APPROX_COUNT_DISTINCT".equals(name)
                    || name.startsWith("PERCENTILE") || "MEDIAN".equals(name);
        }

        private void validateArity(String name, int count) {
            boolean ok;
            switch (name) {
                case "SUM":
                    ok = count >= 2;
                    break;
                case "ABS":
                    ok = count == 1;
                    break;
                case "ROUND":
                case "CEIL":
                case "CEILING":
                case "FLOOR":
                    ok = count == 1 || count == 2;
                    break;
                case "COALESCE":
                case "CONCAT":
                case "GREATEST":
                case "LEAST":
                    ok = count >= 2;
                    break;
                case "IFNULL":
                case "NULLIF":
                    ok = count == 2;
                    break;
                case "TRIM":
                    ok = count == 1 || count == 2;
                    break;
                case "LTRIM":
                case "RTRIM":
                case "LOWER":
                case "UPPER":
                case "LENGTH":
                    ok = count == 1;
                    break;
                case "SUBSTR":
                case "SUBSTRING":
                    ok = count == 2 || count == 3;
                    break;
                case "NVL":
                case "DATE_FORMAT":
                case "STR_TO_DATE":
                case "DATEDIFF":
                case "LEFT":
                case "RIGHT":
                    ok = count == 2;
                    break;
                case "YEAR":
                case "MONTH":
                case "DAY":
                case "DAYOFWEEK":
                case "DAYOFYEAR":
                case "WEEK":
                case "QUARTER":
                case "HOUR":
                case "MINUTE":
                case "SECOND":
                case "INITCAP":
                    ok = count == 1;
                    break;
                case "REPLACE":
                case "LPAD":
                case "RPAD":
                    ok = count == 3;
                    break;
                default:
                    ok = true;
                    break;
            }
            if (!ok) {
                throw syntax("函数 " + name + " 参数个数不正确");
            }
        }

        private static FormulaExpr binary(FormulaExpr left, String op, FormulaExpr right,
                                          int precedence) {
            String l = left.precedence < precedence ? "(" + left.sql + ")" : left.sql;
            String r = right.precedence < precedence ? "(" + right.sql + ")" : right.sql;
            return new FormulaExpr(precedence, l + " " + op + " " + r);
        }

        private FormulaToken peek() {
            return tokens.get(pos);
        }

        private FormulaToken next() {
            FormulaToken token = tokens.get(pos);
            if (token.type != FormulaToken.Type.END) {
                pos++;
            }
            return token;
        }

        private void expect(FormulaToken.Type type, String display) {
            if (peek().type != type) {
                throw syntax("缺少 " + display);
            }
            next();
        }

        private BizException syntax(String detail) {
            return nodeError(node, "FORMULA_SYNTAX", "Formula 表达式语法错误：" + detail);
        }

        private static boolean isIdentStart(char c) {
            return Character.isLetter(c) || c == '_';
        }

        private static boolean isIdentPart(char c) {
            return Character.isLetterOrDigit(c) || c == '_';
        }

        private static boolean hasControlChar(String value) {
            for (int i = 0; i < value.length(); i++) {
                if (value.charAt(i) < 0x20) {
                    return true;
                }
            }
            return false;
        }
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
            String table = sourceTable(node, flowId);
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

    private String sourceTable(DagNode node, long flowId) {
        Long dsId = node.longValue("dataSourceId");
        String table = node.text("tableName", null);
        if (dsId == null || table == null || table.trim().isEmpty()) {
            return null;
        }
        if (sourceTableNameService != null) {
            return sourceTableNameService.resolve(flowId, dsId, table.trim());
        }
        String stable = nameSanitizer.stableSuffix(
                flowId + "|" + dsId + "|" + table.trim().toLowerCase(Locale.ROOT));
        return nameSanitizer.flowSourceTable(flowId, table.trim(), stable);
    }
}
