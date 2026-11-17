package com.datamodel.sink;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.OutputColumn;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Sink 配置解析与静态校验（架构 M3.7 §17/§20.4）。
 * 保存画布时校验静态字段；执行时再用真实上游列做二次校验。
 */
@Component
public class SinkConfigParser {

    private static final Set<String> TARGET_TYPES = new HashSet<>(Arrays.asList(
            "DORIS", "MYSQL", "DM8", "ORACLE"));
    private static final Set<String> WRITE_MODES = new HashSet<>(Arrays.asList(
            "APPEND", "OVERWRITE"));
    private static final Set<String> HIDDEN_COLUMNS = new HashSet<>(Arrays.asList(
            "__dm_rowid", "__dm_sink_seq"));
    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z0-9_$]+");
    private static final Pattern TARGET_COLUMN = Pattern.compile("[A-Za-z0-9_]+");

    public SinkConfig parse(DagNode node) {
        SinkConfig config = new SinkConfig();
        config.setNodeId(node.id());

        String targetType = upper(node.text("targetType", SinkConfig.DORIS));
        if (!TARGET_TYPES.contains(targetType)) {
            throw invalid(node.id(), "TARGET_TYPE_INVALID", "不支持的目标类型：" + targetType);
        }
        config.setTargetType(targetType);

        config.setDataSourceId(node.longValue("dataSourceId"));

        String tableName = node.text("tableName", null);
        config.setTableName(validateTableName(node.id(), tableName, targetType));

        String writeMode = upper(node.text("writeMode", SinkConfig.APPEND));
        if ("UPSERT".equals(writeMode)) {
            throw invalid(node.id(), "WRITE_MODE_UNSUPPORTED",
                    "更新插入（UPSERT）当前版本暂不支持，请选择追加或覆盖");
        }
        if (!WRITE_MODES.contains(writeMode)) {
            throw invalid(node.id(), "WRITE_MODE_INVALID", "不支持的写入模式：" + writeMode);
        }
        config.setWriteMode(writeMode);

        config.setBatchSize(parseBatchSize(node));
        config.setAutoCreateTable(node.boolValue("autoCreateTable", true));

        // DORIS 目标同样必须绑定数据源：Stream Load 写到所选 Doris，而非平台内置库。
        if (config.getDataSourceId() == null) {
            throw invalid(node.id(), "DATA_SOURCE_REQUIRED",
                    config.isDoris() ? "请选择 Doris 目标数据源（Stream Load 目标）" : "请选择目标数据源");
        }

        String watermark = node.text("watermarkColumn", null);
        watermark = watermark == null ? null : watermark.trim();
        config.setWatermarkColumn(watermark == null || watermark.isEmpty() ? null : watermark);

        config.setFieldMapping(parseFields(node));
        return config;
    }

    /**
     * 用真实/逻辑上游字段补全并校验映射。
     * upstreamColumns 为空（Source 未选字段）时跳过字段存在性校验，运行时按物理表补全。
     */
    public SinkConfig resolve(SinkConfig config, List<OutputColumn> upstreamColumns) {
        Map<String, OutputColumn> byName = new LinkedHashMap<>();
        if (upstreamColumns != null) {
            for (OutputColumn column : upstreamColumns) {
                if (column.getName() != null && !HIDDEN_COLUMNS.contains(column.getName().toLowerCase(Locale.ROOT))) {
                    byName.put(column.getName().toLowerCase(Locale.ROOT), column);
                }
            }
        }

        List<SinkField> resolved = new ArrayList<>();
        if (config.getFieldMapping().isEmpty()) {
            for (OutputColumn column : byName.values()) {
                resolved.add(new SinkField(column.getName(), column.getName(), column.getDorisType()));
            }
        } else {
            Set<String> targets = new LinkedHashSet<>();
            for (SinkField field : config.getFieldMapping()) {
                String source = field.getSource() == null ? "" : field.getSource().trim();
                if (source.isEmpty()) {
                    throw invalid(config.getNodeId(), "FIELD_SOURCE_EMPTY", "上游字段不能为空");
                }
                if (HIDDEN_COLUMNS.contains(source.toLowerCase(Locale.ROOT))) {
                    throw invalid(config.getNodeId(), "FIELD_SOURCE_HIDDEN",
                            "字段映射不能包含内部列：" + source);
                }
                OutputColumn upstream = byName.get(source.toLowerCase(Locale.ROOT));
                if (upstream == null && !byName.isEmpty()) {
                    throw invalid(config.getNodeId(), "FIELD_SOURCE_MISSING", "上游字段不存在：" + source);
                }
                String actualSource = upstream == null ? source : upstream.getName();

                String target = field.getTarget() == null || field.getTarget().trim().isEmpty()
                        ? actualSource : field.getTarget().trim();
                if (!TARGET_COLUMN.matcher(target).matches()) {
                    throw invalid(config.getNodeId(), "FIELD_TARGET_INVALID",
                            "目标字段只能包含字母、数字和下划线：" + target);
                }
                if (HIDDEN_COLUMNS.contains(target.toLowerCase(Locale.ROOT))) {
                    throw invalid(config.getNodeId(), "FIELD_TARGET_HIDDEN",
                            "目标字段不能使用内部列名：" + target);
                }
                if (!targets.add(target.toLowerCase(Locale.ROOT))) {
                    throw invalid(config.getNodeId(), "FIELD_TARGET_DUPLICATE", "目标字段重复：" + target);
                }
                String type = field.getType();
                if (type == null || type.trim().isEmpty()) {
                    type = upstream == null ? "STRING" : upstream.getDorisType();
                }
                resolved.add(new SinkField(actualSource, target, type));
            }
        }

        String watermark = config.getWatermarkColumn();
        if (watermark != null) {
            if (!byName.isEmpty()) {
                OutputColumn column = byName.get(watermark.toLowerCase(Locale.ROOT));
                if (column == null) {
                    throw invalid(config.getNodeId(), "WATERMARK_MISSING",
                            "水位线字段必须是上游输出字段");
                }
                config.setWatermarkColumn(column.getName());
            } else {
                // 上游输出列暂不可解析时，用字段映射的 source 兜底校验。
                Set<String> sources = new LinkedHashSet<>();
                for (SinkField field : config.getFieldMapping()) {
                    if (field.getSource() != null) {
                        sources.add(field.getSource().trim().toLowerCase(Locale.ROOT));
                    }
                }
                if (!sources.isEmpty() && !sources.contains(watermark.toLowerCase(Locale.ROOT))) {
                    throw invalid(config.getNodeId(), "WATERMARK_MISSING",
                            "水位线字段必须是上游输出字段");
                }
            }
        }

        if (resolved.isEmpty() && !byName.isEmpty()) {
            throw invalid(config.getNodeId(), "NO_OUTPUT_FIELDS", "上游节点没有可输出的字段");
        }
        config.setFieldMapping(resolved);
        return config;
    }

    /** 运行时物理表校验：fieldMapping.source / watermarkColumn 必须真实存在。 */
    public void validateAgainstPhysicalColumns(SinkConfig config, Set<String> physicalColumns) {
        Set<String> lower = new LinkedHashSet<>();
        for (String column : physicalColumns) {
            lower.add(column.toLowerCase(Locale.ROOT));
        }
        for (SinkField field : config.getFieldMapping()) {
            if (!lower.contains(field.getSource().toLowerCase(Locale.ROOT))) {
                throw invalid(config.getNodeId(), "FIELD_SOURCE_MISSING",
                        "上游字段不存在：" + field.getSource());
            }
        }
        if (config.getWatermarkColumn() != null
                && !lower.contains(config.getWatermarkColumn().toLowerCase(Locale.ROOT))) {
            throw invalid(config.getNodeId(), "WATERMARK_MISSING", "水位线字段必须是上游输出字段");
        }
    }

    public static Set<String> hiddenColumns() {
        return HIDDEN_COLUMNS;
    }

    private List<SinkField> parseFields(DagNode node) {
        List<SinkField> fields = new ArrayList<>();
        JsonNode array = node.array("fieldMapping");
        if (array == null) {
            array = node.array("fields");
        }
        if (array == null) {
            return fields;
        }
        for (JsonNode item : array) {
            String source = item.path("source").asText(item.path("field").asText(""));
            String target = item.path("target").asText(source);
            String type = item.path("type").asText(null);
            fields.add(new SinkField(source == null ? "" : source.trim(),
                    target == null ? "" : target.trim(), type));
        }
        return fields;
    }

    private int parseBatchSize(DagNode node) {
        JsonNode value = node.config() == null ? null : node.config().get("batchSize");
        if (value == null || value.isNull()) {
            return SinkConfig.DEFAULT_BATCH_SIZE;
        }
        int size;
        if (value.isNumber()) {
            size = value.asInt();
        } else {
            try {
                size = Integer.parseInt(value.asText().trim());
            } catch (NumberFormatException e) {
                throw invalid(node.id(), "BATCH_SIZE_INVALID", "批量大小需为 100–100000 之间的整数");
            }
        }
        if (size < SinkConfig.MIN_BATCH_SIZE || size > SinkConfig.MAX_BATCH_SIZE) {
            throw invalid(node.id(), "BATCH_SIZE_INVALID", "批量大小需为 100–100000 之间的整数");
        }
        return size;
    }

    private String validateTableName(String nodeId, String raw, String targetType) {
        if (raw == null || raw.trim().isEmpty()) {
            throw invalid(nodeId, "TABLE_NAME_REQUIRED", "请输入目标表名");
        }
        String table = raw.trim();
        if (table.contains(".")) {
            throw invalid(nodeId, "TABLE_NAME_SCHEMA_UNSUPPORTED",
                    "表名只填表本身，不支持「库名.表名」格式");
        }
        if (!TABLE_NAME.matcher(table).matches()) {
            throw invalid(nodeId, "TABLE_NAME_INVALID", "表名只能包含字母、数字、下划线和 $");
        }
        int max = "DORIS".equals(targetType) || "MYSQL".equals(targetType) ? 64 : 128;
        if (table.length() > max) {
            throw invalid(nodeId, "TABLE_NAME_TOO_LONG", "表名长度不能超过 " + max + " 个字符");
        }
        return table;
    }

    private static String upper(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    public static BizException invalid(String nodeId, String reason, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", nodeId);
        data.put("reason", reason);
        return new BizException(ErrorCode.SINK_CONFIG_INVALID, message, data);
    }
}
