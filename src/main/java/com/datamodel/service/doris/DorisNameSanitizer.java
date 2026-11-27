package com.datamodel.service.doris;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Doris 表名清洗，规则见《数据同步与物理化设计-v1》§1.1。
 * 仅小写 a-z0-9_，连续下划线折叠，超 64 字符截断并追加完整名 MD5 前 6 位。
 */
@Component
public class DorisNameSanitizer {

    private static final int MAX_LEN = 64;
    private static final int HASH_LEN = 6;

    public String sourceTable(long dataSourceId, String sourceTable) {
        return fit("dm_src_" + dataSourceId + "_" + sanitizePart(sourceTable));
    }

    /**
     * 任务源表名：dm_src_{flowId}_{源表名}_{6位随机后缀}。
     * 超长时截断源表名片段并插入源表名 MD5 前 6 位，随机后缀始终保留，总长不超过 64。
     */
    public String flowSourceTable(long flowId, String sourceTable, String suffix) {
        String random = sanitizePart(suffix);
        if (random.length() > HASH_LEN) {
            random = random.substring(0, HASH_LEN);
        }
        String prefix = "dm_src_" + flowId + "_";
        String tail = "_" + random;
        String part = sanitizePart(sourceTable);
        if (prefix.length() + part.length() + tail.length() <= MAX_LEN) {
            return prefix + part + tail;
        }
        int room = MAX_LEN - prefix.length() - HASH_LEN - 2 - random.length();
        if (room < 1) {
            return fit(prefix + random);
        }
        String shortened = part.substring(0, room);
        while (shortened.endsWith("_")) {
            shortened = shortened.substring(0, shortened.length() - 1);
        }
        if (shortened.isEmpty()) {
            shortened = "t";
        }
        return prefix + shortened + "_" + md5(part).substring(0, HASH_LEN) + tail;
    }

    public String middleTable(long flowId, String nodeId) {
        return fit("dm_mid_" + flowId + "_" + sanitizePart(nodeId));
    }

    /** 某任务全部中间表的公共前缀（用于保存画布时对账清理）。 */
    public String middleTablePrefix(long flowId) {
        return "dm_mid_" + flowId + "_";
    }

    /** 某任务全部源表副本的公共前缀（用于保存画布/删除任务时对账清理）。 */
    public String sourceTablePrefix(long flowId) {
        return "dm_src_" + flowId + "_";
    }

    /** 大模型节点的 id→结果映射临时表：dm_llm_{flowId}_{nodeId}__map。 */
    public String llmMapTable(long flowId, String nodeId) {
        return fit("dm_llm_" + flowId + "_" + sanitizePart(nodeId) + "__map");
    }

    /** 某任务全部大模型临时表的公共前缀（用于删除任务时清理）。 */
    public String llmTablePrefix(long flowId) {
        return "dm_llm_" + flowId + "_";
    }

    /** 清洗任意片段：小写、非 [a-z0-9_] 变 _、折叠下划线、去首尾下划线。 */
    public String sanitizePart(String raw) {
        if (raw == null) {
            return "unknown";
        }
        String lower = raw.trim().toLowerCase();
        StringBuilder sb = new StringBuilder(lower.length());
        boolean lastUnderscore = false;
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
            char out = ok ? c : '_';
            if (out == '_') {
                if (lastUnderscore) {
                    continue;
                }
                lastUnderscore = true;
            } else {
                lastUnderscore = false;
            }
            sb.append(out);
        }
        String cleaned = sb.toString();
        while (cleaned.startsWith("_")) {
            cleaned = cleaned.substring(1);
        }
        while (cleaned.endsWith("_")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1);
        }
        return cleaned.isEmpty() ? "unknown" : cleaned;
    }

    /** 清洗列名；保留可用标识符，否则按序号生成稳定列名。 */
    public String columnName(String raw, int index) {
        if (raw != null && raw.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return raw;
        }
        return "col_" + index;
    }

    private String fit(String candidate) {
        if (candidate.getBytes(StandardCharsets.UTF_8).length <= MAX_LEN) {
            return candidate;
        }
        String hash = md5(candidate).substring(0, HASH_LEN);
        int keep = MAX_LEN - HASH_LEN - 1;
        String truncated = candidate.substring(0, Math.min(candidate.length(), keep));
        while (truncated.endsWith("_")) {
            truncated = truncated.substring(0, truncated.length() - 1);
        }
        return truncated + "_" + hash;
    }

    /** 确定性 6 位后缀（元数据不可用时的稳定兜底）。 */
    public String stableSuffix(String value) {
        return md5(value == null ? "" : value).substring(0, HASH_LEN);
    }

    static String md5(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
