package com.datamodel.service.llm;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.Identifiers;
import com.datamodel.sql.OutputColumn;
import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 大模型节点配置（协议 v2，见需求 2026-11-26 §5）。
 * Key 不在此配置中，只存 data_model_llm_credential。
 */
public final class LlmConfig {

    public static final int DEFAULT_BATCH_ROWS = 20;
    public static final int MIN_BATCH_ROWS = 1;
    public static final int MAX_BATCH_ROWS = 200;
    public static final int DEFAULT_TIMEOUT_SECONDS = 60;
    public static final int MAX_TIMEOUT_SECONDS = 600;
    public static final int DEFAULT_MAX_RETRIES = 2;
    public static final int MAX_RETRIES = 5;
    public static final String PLACEHOLDER_INPUT_TEXT = "${input_text}";
    public static final String PLACEHOLDER_ENUM_LIST = "${enum_list}";
    public static final String PLACEHOLDER_FALLBACK = "${fallback}";
    public static final Set<String> RESERVED_VARIABLES = Set.of("input_text", "enum_list", "fallback");
    private static final Pattern VARIABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** 批量模式：模板含 ${input_text}；逐行模式：不含 input_text 但含字段变量。 */
    public enum Mode {
        BATCH, ROW
    }

    private final String nodeId;
    private final String alias;
    private final List<String> inputFields;
    private final List<LlmVariable> variables;
    private final List<String> fetchFields;
    private final String outputField;
    private final List<String> enumValues;
    private final int batchRows;
    private final String endpoint;
    private final String model;
    private final String promptTemplate;
    private final String unknownValue;
    private final int timeoutSeconds;
    private final int maxRetries;

    private LlmConfig(String nodeId, String alias, List<String> inputFields,
                      List<LlmVariable> variables, List<String> fetchFields, String outputField,
                      List<String> enumValues, int batchRows, String endpoint, String model,
                      String promptTemplate, String unknownValue, int timeoutSeconds,
                      int maxRetries) {
        this.nodeId = nodeId;
        this.alias = alias;
        this.inputFields = inputFields;
        this.variables = variables;
        this.fetchFields = fetchFields;
        this.outputField = outputField;
        this.enumValues = enumValues;
        this.batchRows = batchRows;
        this.endpoint = endpoint;
        this.model = model;
        this.promptTemplate = promptTemplate;
        this.unknownValue = unknownValue;
        this.timeoutSeconds = timeoutSeconds;
        this.maxRetries = maxRetries;
    }

    public static LlmConfig parse(DagNode node) {
        JsonNode config = node.config();
        String alias = text(config, "alias", "a");
        if (alias.isEmpty()) {
            alias = "a";
        }
        String unknown = text(config, "unknownValue", "未知");
        if (unknown.isEmpty()) {
            unknown = "未知";
        }
        List<String> inputFields = stringList(config.get("inputFields"));
        List<LlmVariable> variables = variables(config.get("inputVariables"), inputFields);
        return new LlmConfig(
                node.id(),
                alias,
                inputFields,
                variables,
                mergeFields(inputFields, variables),
                text(config, "outputField", ""),
                stringListKeepBlank(config.get("enumValues")),
                intValue(config.get("batchRows"), DEFAULT_BATCH_ROWS),
                text(config, "endpoint", ""),
                text(config, "model", ""),
                textRaw(config, "promptTemplate", ""),
                unknown,
                intValue(config.get("timeoutSeconds"), DEFAULT_TIMEOUT_SECONDS),
                intValue(config.get("maxRetries"), DEFAULT_MAX_RETRIES));
    }

    /** inputVariables 优先；缺省（旧配置）时每个输入字段一行、变量名=字段名。 */
    private static List<LlmVariable> variables(JsonNode array, List<String> inputFields) {
        List<LlmVariable> out = new ArrayList<>();
        if (array != null && array.isArray() && !array.isEmpty()) {
            for (JsonNode item : array) {
                if (item == null || !item.isObject()) {
                    continue;
                }
                String field = item.path("field").asText("").trim();
                if (field.isEmpty()) {
                    continue;
                }
                String variable = item.path("variable").asText("").trim();
                out.add(new LlmVariable(field, variable.isEmpty() ? field : variable));
            }
            return out;
        }
        for (String field : inputFields) {
            out.add(new LlmVariable(field, field));
        }
        return out;
    }

    /** 上游取数字段：保留 inputFields 顺序，再补 inputVariables 中独有字段。 */
    private static List<String> mergeFields(List<String> inputFields, List<LlmVariable> variables) {
        List<String> out = new ArrayList<>(inputFields);
        for (LlmVariable variable : variables) {
            boolean exists = false;
            for (String field : out) {
                if (field.equalsIgnoreCase(variable.field())) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                out.add(variable.field());
            }
        }
        return out;
    }

    /** 画布自动保存可能带空节点；只有填写过任何配置才做严格校验。 */
    public static boolean isPlaceholder(DagNode node) {
        JsonNode config = node.config();
        if (config == null || !config.isObject()) {
            return true;
        }
        String[] keys = {"alias", "inputFields", "inputVariables", "outputField", "enumValues",
                "endpoint", "model", "promptTemplate", "unknownValue", "batchRows",
                "timeoutSeconds", "maxRetries"};
        for (String key : keys) {
            JsonNode value = config.get(key);
            if (value == null || value.isNull()) {
                continue;
            }
            if (value.isArray() && value.size() > 0) {
                return false;
            }
            if (!value.isArray() && !value.asText().trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public void validate(List<OutputColumn> upstream) {
        if (endpoint.isEmpty()) {
            throw error("LLM_ENDPOINT_EMPTY", "大模型节点的模型 endpoint 不能为空");
        }
        checkEndpoint(endpoint);
        if (model.isEmpty()) {
            throw error("LLM_MODEL_EMPTY", "大模型节点的 model 不能为空");
        }
        if (variables.isEmpty()) {
            throw error("LLM_INPUT_FIELDS_EMPTY", "大模型节点至少需要选择 1 个输入字段");
        }
        if (upstream == null || upstream.isEmpty()) {
            throw error("LLM_INPUT_FIELD_UNKNOWN",
                    "大模型节点无法确定上游字段，请先配置上游节点并获取字段");
        }
        for (LlmVariable variable : variables) {
            if (findColumn(upstream, variable.field()) == null) {
                throw error("LLM_INPUT_FIELD_UNKNOWN",
                        "大模型节点引用了不存在的输入字段：" + variable.field());
            }
            String name = variable.variable();
            if (!VARIABLE_NAME.matcher(name).matches()) {
                throw error("LLM_VAR_INVALID", "变量名不合法：" + name
                        + "，需以字母或下划线开头，只含字母、数字、下划线");
            }
            if (RESERVED_VARIABLES.contains(name)) {
                throw error("LLM_VAR_INVALID", "变量名不能与内置变量重名：" + name
                        + "（保留：${input_text}、${enum_list}、${fallback}）");
            }
        }
        Set<String> seenVars = new LinkedHashSet<>();
        for (LlmVariable variable : variables) {
            if (!seenVars.add(variable.variable())) {
                throw error("LLM_VAR_DUP", "变量名重复：" + variable.variable());
            }
        }
        if (outputField.isEmpty()) {
            throw error("LLM_OUTPUT_FIELD_EMPTY", "大模型节点的新字段名不能为空");
        }
        try {
            Identifiers.requireField(outputField);
        } catch (BizException e) {
            throw error("LLM_OUTPUT_FIELD_EMPTY", "大模型节点的新字段名不合法：" + outputField);
        }
        if (findColumn(upstream, outputField) != null) {
            throw error("LLM_OUTPUT_FIELD_DUP", "大模型节点的新字段与上游已有列重名：" + outputField);
        }
        if (mode() == null) {
            throw error("LLM_TEMPLATE_NO_DATA",
                    "Prompt 模板必须包含 ${input_text}（批量模式）或至少一个字段变量（逐行模式），"
                            + "当前模板两者都没有");
        }
        if (batchRows < MIN_BATCH_ROWS || batchRows > MAX_BATCH_ROWS) {
            throw error("LLM_BATCH_ROWS_OUT_OF_RANGE",
                    "大模型的批大小需为 " + MIN_BATCH_ROWS + "–" + MAX_BATCH_ROWS + " 的整数");
        }
        if (timeoutSeconds < 1 || timeoutSeconds > MAX_TIMEOUT_SECONDS) {
            throw error("LLM_TIMEOUT_OUT_OF_RANGE", "大模型的超时秒数需为 1–" + MAX_TIMEOUT_SECONDS);
        }
        if (maxRetries < 0 || maxRetries > MAX_RETRIES) {
            throw error("LLM_MAX_RETRIES_OUT_OF_RANGE", "大模型的最大重试次数需为 0–" + MAX_RETRIES);
        }
        for (String value : enumValues) {
            if (value.trim().isEmpty()) {
                throw error("LLM_ENUM_INVALID", "大模型的枚举值不能包含空行");
            }
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String value : enumValues) {
            if (!seen.add(value.toLowerCase(Locale.ROOT))) {
                throw error("LLM_ENUM_INVALID", "大模型的枚举值重复：" + value);
            }
        }
    }

    /** 模板模式；两种占位符都没有时返回 null（交 validate 报 LLM_TEMPLATE_NO_DATA）。 */
    public Mode mode() {
        if (promptTemplate.contains(PLACEHOLDER_INPUT_TEXT)) {
            return Mode.BATCH;
        }
        for (LlmVariable variable : variables) {
            if (promptTemplate.contains("${" + variable.variable() + "}")) {
                return Mode.ROW;
            }
        }
        return null;
    }

    public boolean rowMode() {
        return mode() == Mode.ROW;
    }

    private void checkEndpoint(String value) {
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            throw error("LLM_ENDPOINT_INVALID", "大模型的 endpoint 不是合法 URL：" + value);
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw error("LLM_ENDPOINT_INVALID", "大模型的 endpoint 仅支持 http/https：" + value);
        }
        if (uri.getHost() == null || uri.getHost().trim().isEmpty()) {
            throw error("LLM_ENDPOINT_INVALID", "大模型的 endpoint 缺少主机名：" + value);
        }
    }

    public BizException error(String reason, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nodeId", nodeId);
        data.put("reason", reason);
        return new BizException(ErrorCode.PARAM_ERROR, message, data);
    }

    static OutputColumn findColumn(List<OutputColumn> columns, String name) {
        for (OutputColumn column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
    }

    /** BaseURL 自动补 /chat/completions；已是完整 endpoint（大小写不敏感）则原样使用。 */
    public String chatUrl() {
        String base = endpoint == null ? "" : endpoint.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        if (base.toLowerCase(Locale.ROOT).endsWith("/chat/completions")) {
            return base;
        }
        return base + "/chat/completions";
    }

    /** endpoint → 模型列表地址：去掉 /chat/completions，或以 /v1 收尾补 /models。 */
    public static String normalizeModelsUrl(String endpoint) {
        String base = endpoint == null ? "" : endpoint.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        if (base.toLowerCase(Locale.ROOT).endsWith("/models")) {
            return base;
        }
        String suffix = "/chat/completions";
        if (base.toLowerCase(Locale.ROOT).endsWith(suffix)) {
            base = base.substring(0, base.length() - suffix.length());
            while (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
        }
        return base + "/models";
    }

    /** 批量模式（row == null）或逐行模式渲染；未匹配的变量占位符不保留。 */
    public String renderPrompt(String inputText, LlmRow row) {
        String enums = enumValues.isEmpty() ? "" : String.join("\n", prefixEach(enumValues, "- "));
        String out = promptTemplate
                .replace(PLACEHOLDER_ENUM_LIST, enums)
                .replace(PLACEHOLDER_FALLBACK, unknownValue)
                .replace(PLACEHOLDER_INPUT_TEXT, inputText == null ? "" : inputText);
        for (LlmVariable variable : variables) {
            String value = row == null ? "" : row.value(variable.field());
            out = out.replace("${" + variable.variable() + "}", value == null ? "" : value);
        }
        return out;
    }

    public String renderPrompt(String inputText) {
        return renderPrompt(inputText, null);
    }

    private static List<String> prefixEach(List<String> values, String prefix) {
        List<String> out = new ArrayList<>(values.size());
        for (String value : values) {
            out.add(prefix + value);
        }
        return out;
    }

    private static List<String> stringList(JsonNode array) {
        List<String> values = new ArrayList<>();
        if (array == null || !array.isArray()) {
            return values;
        }
        for (JsonNode item : array) {
            if (item == null || item.isNull()) {
                continue;
            }
            String value = item.asText().trim();
            if (!value.isEmpty()) {
                values.add(value);
            }
        }
        return values;
    }

    /** 枚举值保留空串，交给 validate 明确报 LLM_ENUM_INVALID，而不是静默丢弃。 */
    private static List<String> stringListKeepBlank(JsonNode array) {
        List<String> values = new ArrayList<>();
        if (array == null || !array.isArray()) {
            return values;
        }
        for (JsonNode item : array) {
            if (item == null || item.isNull()) {
                continue;
            }
            values.add(item.asText().trim());
        }
        return values;
    }

    private static String text(JsonNode config, String field, String fallback) {
        return textRaw(config, field, fallback).trim();
    }

    private static String textRaw(JsonNode config, String field, String fallback) {
        JsonNode value = config == null ? null : config.get(field);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    private static int intValue(JsonNode value, int fallback) {
        if (value == null || value.isNull()) {
            return fallback;
        }
        if (value.isNumber()) {
            return value.asInt();
        }
        try {
            return Integer.parseInt(value.asText().trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public String nodeId() {
        return nodeId;
    }

    public String alias() {
        return alias;
    }

    public List<String> inputFields() {
        return inputFields;
    }

    public List<LlmVariable> variables() {
        return variables;
    }

    /** 上游实际取数列表（inputFields ∪ inputVariables.field）。 */
    public List<String> fetchFields() {
        return fetchFields;
    }

    public String outputField() {
        return outputField;
    }

    public List<String> enumValues() {
        return enumValues;
    }

    public int batchRows() {
        return batchRows;
    }

    public String endpoint() {
        return endpoint;
    }

    public String model() {
        return model;
    }

    public String promptTemplate() {
        return promptTemplate;
    }

    public String unknownValue() {
        return unknownValue;
    }

    public int timeoutSeconds() {
        return timeoutSeconds;
    }

    public int maxRetries() {
        return maxRetries;
    }
}
