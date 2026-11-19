package com.datamodel.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.crypto.AesGcmCipher;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.domain.FlowEntity;
import com.datamodel.domain.FlowRunEntity;
import com.datamodel.domain.NodeRunEntity;
import com.datamodel.dto.NodeErrorDetailVO;
import com.datamodel.mapper.FlowRunMapper;
import com.datamodel.mapper.NodeRunMapper;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.sql.DagNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 节点错误详情：只读 node_run，并从 Doris Stream Load 的 ErrorURL 安全抓取错误日志。
 * 抓取限制：仅 http/https、5 秒超时、原始响应 256KB、最终日志 64KB、禁止跨主机重定向。
 */
@Service
public class NodeErrorDetailService {

    private static final Logger log = LoggerFactory.getLogger(NodeErrorDetailService.class);

    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 5000;
    private static final int MAX_RAW_BYTES = 256 * 1024;
    private static final int MAX_LOG_CHARS = 64 * 1024;
    private static final int MAX_REDIRECTS = 3;

    private static final Pattern URL_PATTERN = Pattern.compile(
            "https?://[^\\s\"'<>，,、）)】]+", Pattern.CASE_INSENSITIVE);
    private static final Pattern SCRIPT_STYLE = Pattern.compile(
            "(?is)<(script|style)\\b[^>]*>.*?</\\1>");
    private static final Pattern TAG = Pattern.compile("(?s)<[^>]*>");
    private static final Pattern NUMERIC_ENTITY = Pattern.compile("&#(x[0-9a-fA-F]+|\\d+);");

    private final FlowService flowService;
    private final FlowRunMapper flowRunMapper;
    private final NodeRunMapper nodeRunMapper;
    private final DataSourceService dataSourceService;
    private final AesGcmCipher aesGcmCipher;
    private final DorisClient dorisClient;

    public NodeErrorDetailService(FlowService flowService, FlowRunMapper flowRunMapper,
                                  NodeRunMapper nodeRunMapper, DataSourceService dataSourceService,
                                  AesGcmCipher aesGcmCipher, DorisClient dorisClient) {
        this.flowService = flowService;
        this.flowRunMapper = flowRunMapper;
        this.nodeRunMapper = nodeRunMapper;
        this.dataSourceService = dataSourceService;
        this.aesGcmCipher = aesGcmCipher;
        this.dorisClient = dorisClient;
    }

    public NodeErrorDetailVO detail(Long runId, String nodeId) {
        FlowRunEntity run = flowRunMapper.selectById(runId);
        if (run == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "执行记录不存在");
        }
        NodeRunEntity nr = nodeRunMapper.selectOne(new LambdaQueryWrapper<NodeRunEntity>()
                .eq(NodeRunEntity::getFlowRunId, runId)
                .eq(NodeRunEntity::getNodeId, nodeId)
                .last("LIMIT 1"));
        if (nr == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "该节点没有执行记录");
        }

        NodeErrorDetailVO vo = new NodeErrorDetailVO();
        vo.setRunId(runId);
        vo.setNodeId(nr.getNodeId());
        vo.setNodeName(nr.getNodeName());
        vo.setStatus(nr.getStatus());
        vo.setErrorCode(nr.getErrorCode());
        vo.setFailStage(nr.getFailStage());
        vo.setErrMsg(nr.getErrMsg());
        vo.setSqlText(nr.getSqlText());
        vo.setSqlParams(nr.getParams());

        if (!isSink(nr.getNodeType()) || !isDorisSink(run, nodeId)) {
            return vo;
        }
        String errorUrl = extractErrorUrl(nr.getErrMsg());
        if (errorUrl == null) {
            return vo;
        }
        vo.setErrorUrl(errorUrl);

        DataSourceEntity target = dorisTarget(run, nodeId);
        Supplier<String[]> auth = target == null ? null : () -> auth(target);
        FetchOutcome outcome = fetch(errorUrl, auth);
        if (outcome.error != null && target != null) {
            String rebased = rebaseToTarget(errorUrl, target);
            if (rebased != null && !rebased.equals(errorUrl)) {
                FetchOutcome retried = fetch(rebased, auth);
                if (retried.error == null) {
                    outcome = retried;
                }
            }
        }
        if (outcome.error != null) {
            vo.setFetchError(outcome.error);
        } else {
            vo.setStreamLoadLog(outcome.text);
            vo.setStreamLoadLogTruncated(outcome.truncated);
        }
        return vo;
    }

    private static boolean isSink(String nodeType) {
        return nodeType != null && "sink".equalsIgnoreCase(nodeType.trim());
    }

    static String extractErrorUrl(String errMsg) {
        if (errMsg == null || errMsg.isEmpty()) {
            return null;
        }
        Matcher matcher = URL_PATTERN.matcher(errMsg);
        String fallback = null;
        while (matcher.find()) {
            String url = matcher.group().replace("&amp;", "&");
            if (url.contains("_load_error_log")) {
                return url;
            }
            if (fallback == null && url.toLowerCase(Locale.ROOT).contains("/api/")) {
                fallback = url;
            }
        }
        return fallback;
    }

    /** 只有 DORIS 目标 Sink 的 err_msg 才可能带 Stream Load ErrorURL。 */
    private boolean isDorisSink(FlowRunEntity run, String nodeId) {
        try {
            Long flowId = run.getFlowId();
            if (flowId == null) {
                return false;
            }
            FlowEntity flow = flowService.getEntity(flowId);
            JsonNode nodes = flowService.parseDag(flow).path("nodes");
            if (!nodes.isArray()) {
                return false;
            }
            for (JsonNode n : nodes) {
                if (!nodeId.equals(n.path("id").asText())) {
                    continue;
                }
                DagNode node = DagNode.from(n);
                if (!isSink(node.type())) {
                    return false;
                }
                String targetType = node.text("targetType", "DORIS");
                return "DORIS".equalsIgnoreCase(
                        targetType == null ? "DORIS" : targetType.trim());
            }
        } catch (RuntimeException e) {
            log.warn("解析错误详情目标类型失败 run={} node={}：{}", run.getId(), nodeId,
                    e.getMessage());
        }
        return false;
    }

    /** 目标 Doris 数据源；缺失或类型不符返回 null。 */
    private DataSourceEntity dorisTarget(FlowRunEntity run, String nodeId) {
        try {
            Long flowId = run.getFlowId();
            if (flowId == null) {
                return null;
            }
            FlowEntity flow = flowService.getEntity(flowId);
            JsonNode nodes = flowService.parseDag(flow).path("nodes");
            if (!nodes.isArray()) {
                return null;
            }
            for (JsonNode n : nodes) {
                if (!nodeId.equals(n.path("id").asText())) {
                    continue;
                }
                DagNode node = DagNode.from(n);
                if (!isSink(node.type())) {
                    return null;
                }
                Long dsId = node.longValue("dataSourceId");
                if (dsId == null) {
                    return null;
                }
                DataSourceEntity ds = dataSourceService.getEntity(dsId);
                if (ds.getType() == null
                        || !"DORIS".equalsIgnoreCase(ds.getType().trim())) {
                    return null;
                }
                return ds;
            }
        } catch (RuntimeException e) {
            log.warn("解析错误详情目标数据源失败 run={} node={}：{}", run.getId(), nodeId,
                    e.getMessage());
        }
        return null;
    }

    /** 凭据延迟到 401/403 才解密，且永不写入日志或响应。 */
    private String[] auth(DataSourceEntity ds) {
        try {
            String password = aesGcmCipher.decrypt(ds.getPasswordEnc());
            return new String[]{
                    ds.getUsername() == null ? "" : ds.getUsername(),
                    password == null ? "" : password};
        } catch (RuntimeException e) {
            log.warn("解析错误详情认证凭据失败 ds={}：{}", ds.getId(), e.getMessage());
            return null;
        }
    }

    /** 用数据源配置的 host/httpPort 重建同一 ErrorURL，兼容 BE 返回内网地址的场景。 */
    private String rebaseToTarget(String rawUrl, DataSourceEntity target) {
        try {
            URL url = new URL(rawUrl);
            String host = target.getHost();
            if (host == null || host.trim().isEmpty()) {
                return null;
            }
            Integer port = dorisClient.configuredHttpPort(target);
            int effectivePort = port != null && port > 0 ? port : url.getPort();
            return new URL(url.getProtocol(), host.trim(), effectivePort, url.getFile())
                    .toString();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    static final class FetchOutcome {
        final String text;
        final boolean truncated;
        final String error;

        private FetchOutcome(String text, boolean truncated, String error) {
            this.text = text;
            this.truncated = truncated;
            this.error = error;
        }

        static FetchOutcome ok(String text, boolean truncated) {
            return new FetchOutcome(text, truncated, null);
        }

        static FetchOutcome fail(String error) {
            return new FetchOutcome(null, false, error);
        }
    }

    private FetchOutcome fetch(String rawUrl, Supplier<String[]> auth) {
        String current = rawUrl;
        String[] credential = null;
        try {
            for (int redirects = 0; ; redirects++) {
                URL url = new URL(current);
                String scheme = url.getProtocol() == null
                        ? "" : url.getProtocol().toLowerCase(Locale.ROOT);
                if (!"http".equals(scheme) && !"https".equals(scheme)) {
                    return FetchOutcome.fail("ErrorURL 仅支持 http/https 协议");
                }
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setInstanceFollowRedirects(false);
                conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
                conn.setReadTimeout(READ_TIMEOUT_MS);
                conn.setRequestMethod("GET");
                conn.setRequestProperty("Accept", "*/*");
                if (credential != null) {
                    conn.setRequestProperty("Authorization", "Basic " + basicAuth(credential));
                }

                int code = conn.getResponseCode();
                if (code >= 300 && code < 400) {
                    String location = conn.getHeaderField("Location");
                    conn.disconnect();
                    if (location == null || location.trim().isEmpty()) {
                        return FetchOutcome.fail("ErrorURL 返回重定向但缺少 Location");
                    }
                    if (redirects >= MAX_REDIRECTS) {
                        return FetchOutcome.fail("ErrorURL 重定向次数过多");
                    }
                    URL next = new URL(url, location.trim());
                    if (!sameHost(url, next)) {
                        return FetchOutcome.fail("ErrorURL 重定向到其他主机，已拒绝抓取");
                    }
                    current = next.toString();
                    continue;
                }
                if ((code == 401 || code == 403) && credential == null) {
                    conn.disconnect();
                    credential = auth == null ? null : auth.get();
                    if (credential == null) {
                        return FetchOutcome.fail("ErrorURL 返回 HTTP " + code
                                + "，且无法取得目标 Doris 数据源凭据");
                    }
                    continue;
                }

                Body body = readLimited(code >= 400 ? conn.getErrorStream() : conn.getInputStream());
                String contentType = conn.getContentType();
                conn.disconnect();
                if (code < 200 || code >= 300) {
                    return FetchOutcome.fail("ErrorURL 抓取失败：HTTP " + code);
                }
                Normalized normalized = normalize(body.bytes, contentType);
                return FetchOutcome.ok(normalized.text, body.truncated || normalized.truncated);
            }
        } catch (IOException e) {
            return FetchOutcome.fail("ErrorURL 抓取失败：" + safeMessage(e));
        } catch (RuntimeException e) {
            return FetchOutcome.fail("ErrorURL 抓取失败：" + safeMessage(e));
        }
    }

    private static String basicAuth(String[] credential) {
        String token = credential[0] + ":" + credential[1];
        return Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean sameHost(URL first, URL second) {
        String a = first.getHost();
        String b = second.getHost();
        return a != null && b != null && a.equalsIgnoreCase(b);
    }

    private static final class Body {
        final byte[] bytes;
        final boolean truncated;

        Body(byte[] bytes, boolean truncated) {
            this.bytes = bytes;
            this.truncated = truncated;
        }
    }

    private static Body readLimited(InputStream in) throws IOException {
        if (in == null) {
            return new Body(new byte[0], false);
        }
        try (InputStream input = in) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int n;
            while ((n = input.read(buf)) >= 0) {
                if (out.size() + n > MAX_RAW_BYTES) {
                    int keep = MAX_RAW_BYTES - out.size();
                    if (keep > 0) {
                        out.write(buf, 0, keep);
                    }
                    return new Body(out.toByteArray(), true);
                }
                out.write(buf, 0, n);
            }
            return new Body(out.toByteArray(), false);
        }
    }

    static final class Normalized {
        final String text;
        final boolean truncated;

        Normalized(String text, boolean truncated) {
            this.text = text;
            this.truncated = truncated;
        }
    }

    static Normalized normalize(byte[] raw, String contentType) {
        String text = new String(raw, StandardCharsets.UTF_8);
        String type = contentType == null ? "" : contentType.toLowerCase(Locale.ROOT);
        String trimmed = text.trim();
        if (type.contains("json") || trimmed.startsWith("{") || trimmed.startsWith("[")) {
            text = jsonMessage(trimmed);
        } else if (type.contains("html") || looksLikeHtml(trimmed)) {
            text = htmlToText(text);
        }
        if (text.length() > MAX_LOG_CHARS) {
            return new Normalized(text.substring(0, MAX_LOG_CHARS), true);
        }
        return new Normalized(text, false);
    }

    private static boolean looksLikeHtml(String text) {
        String lower = text.toLowerCase(Locale.ROOT);
        return lower.contains("<html") || lower.contains("<body") || lower.contains("<div")
                || lower.contains("<table") || lower.contains("<pre");
    }

    private static String jsonMessage(String json) {
        try {
            JsonNode node = com.datamodel.common.JsonUtils.mapper().readTree(json);
            if (node != null && node.isObject()) {
                for (String field : new String[]{"message", "Message", "msg", "msgText"}) {
                    JsonNode value = node.get(field);
                    if (value != null && !value.isNull()) {
                        return value.isTextual() ? value.asText() : value.toString();
                    }
                }
            }
        } catch (Exception ignored) {
            // 非合法 JSON 时按原文返回，保留上游内容。
        }
        return json;
    }

    static String htmlToText(String html) {
        String withoutScripts = SCRIPT_STYLE.matcher(html).replaceAll(" ");
        String withoutTags = TAG.matcher(withoutScripts).replaceAll(" ");
        String decoded = decodeEntities(withoutTags);
        return decoded.replaceAll("[\\t\\x0B\\f\\r ]+", " ")
                .replaceAll("\\n{3,}", "\n\n")
                .trim();
    }

    private static String decodeEntities(String text) {
        Matcher matcher = NUMERIC_ENTITY.matcher(text);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String value = matcher.group(1);
            int code;
            try {
                code = value.startsWith("x") || value.startsWith("X")
                        ? Integer.parseInt(value.substring(1), 16)
                        : Integer.parseInt(value);
            } catch (NumberFormatException e) {
                code = -1;
            }
            String replacement = code > 0 && code <= Character.MAX_CODE_POINT
                    ? new String(Character.toChars(code)) : matcher.group();
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString()
                .replace("&nbsp;", " ")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&apos;", "'")
                .replace("&amp;", "&");
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return e.getClass().getSimpleName();
        }
        return message.length() > 500 ? message.substring(0, 500) : message;
    }
}
