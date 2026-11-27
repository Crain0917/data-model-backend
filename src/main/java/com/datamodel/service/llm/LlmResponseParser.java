package com.datamodel.service.llm;

import com.datamodel.common.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 模型响应解析：栈式扫描第一个完整 JSON 数组（容忍代码块与前后噪声），
 * 按 id 字符串匹配（不按返回顺序），重复 id 后者覆盖，缺失/非枚举 → unknownValue。
 */
public final class LlmResponseParser {

    public static final int VALUE_MAX_CHARS = 4096;

    public ParsedBatch parse(String content, List<Long> expectedIds, LlmConfig config) {
        String array = extractJsonArray(content);
        if (array == null) {
            throw new LlmException("LLM_PARSE_FAILED", "模型返回内容不是合法的 JSON 数组");
        }
        JsonNode root;
        try {
            root = JsonUtils.mapper().readTree(array);
        } catch (Exception e) {
            throw new LlmException("LLM_PARSE_FAILED", "模型返回内容不是合法的 JSON 数组");
        }

        Set<Long> expected = new LinkedHashSet<>(expectedIds);
        Map<Long, String> values = new LinkedHashMap<>();
        for (Long id : expected) {
            values.put(id, config.unknownValue());
        }
        Set<Long> seen = new LinkedHashSet<>();
        int returned = 0;
        int invalid = 0;
        int duplicate = 0;
        for (JsonNode item : root) {
            if (!item.isObject()) {
                invalid++;
                continue;
            }
            returned++;
            Long id = parseId(item.get("id"));
            if (id == null || !expected.contains(id)) {
                invalid++;
                continue;
            }
            String value = normalizeValue(item.get("value"));
            String resolved = resolve(value, config);
            if (resolved == null) {
                invalid++;
                resolved = config.unknownValue();
            }
            if (seen.contains(id)) {
                duplicate++;
            } else {
                seen.add(id);
            }
            values.put(id, resolved);
        }
        int missing = expected.size() - seen.size();
        return new ParsedBatch(values, expected.size(), returned, seen.size(), missing, invalid,
                duplicate);
    }

    /** 返回 null 表示不是合法 JSON 数组。 */
    public static String extractJsonArray(String content) {
        if (content == null || content.isEmpty()) {
            return null;
        }
        int start = content.indexOf('[');
        while (start >= 0) {
            String candidate = scanArray(content, start);
            if (candidate != null) {
                try {
                    JsonNode node = JsonUtils.mapper().readTree(candidate);
                    if (node != null && node.isArray()) {
                        return candidate;
                    }
                } catch (Exception ignored) {
                    // 继续找下一个 [
                }
            }
            start = content.indexOf('[', start + 1);
        }
        return null;
    }

    private static String scanArray(String text, int start) {
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            if (c == '"') {
                inString = true;
            } else if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return text.substring(start, i + 1);
                }
                if (depth < 0) {
                    return null;
                }
            }
        }
        return null;
    }

    private static Long parseId(JsonNode idNode) {
        if (idNode == null || idNode.isNull()) {
            return null;
        }
        String text = idNode.isTextual() ? idNode.asText().trim() : idNode.asText();
        if (text == null || text.isEmpty()) {
            return null;
        }
        try {
            return Long.valueOf(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String normalizeValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return "";
        }
        String text = valueNode.isTextual() ? valueNode.asText() : valueNode.toString();
        if (text == null) {
            return "";
        }
        text = text.trim();
        if (text.length() > VALUE_MAX_CHARS) {
            text = text.substring(0, VALUE_MAX_CHARS);
        }
        return text;
    }

    /** 命中枚举返回枚举原值（大小写不敏感）；空值/非枚举返回 null。 */
    private static String resolve(String value, LlmConfig config) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        List<String> enums = config.enumValues();
        if (enums.isEmpty()) {
            return value;
        }
        String lower = value.toLowerCase(Locale.ROOT);
        for (String allowed : enums) {
            if (allowed.trim().toLowerCase(Locale.ROOT).equals(lower)) {
                return allowed.trim();
            }
        }
        return null;
    }

    public record ParsedBatch(Map<Long, String> values, int expected, int returned, int matched,
                              int missing, int invalid, int duplicate) {
    }
}
