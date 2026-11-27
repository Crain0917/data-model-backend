package com.datamodel.service.llm;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.JsonUtils;
import com.datamodel.config.AppProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * OpenAI 兼容 chat/completions 调用：SSRF 校验、超时、401/403 立即失败、
 * 429/5xx/超时按 maxRetries 指数退避。异常信息不含 Key 与 Authorization 头。
 */
@Component
public class LlmClient {

    private static final Logger log = LoggerFactory.getLogger(LlmClient.class);
    private static final int MIN_TIMEOUT = 5;
    private static final int MAX_TIMEOUT = 300;
    private static final int SNIPPET = 300;

    private final AppProperties appProperties;
    /** 单测可调小，避免真的等待 1s/2s。 */
    private long retryBaseMillis = 1000L;

    public LlmClient(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    void setRetryBaseMillis(long retryBaseMillis) {
        this.retryBaseMillis = Math.max(0L, retryBaseMillis);
    }

    public ChatResult chat(LlmConfig config, String apiKey, String prompt,
                           int timeoutSeconds, int maxRetries) {
        checkEndpoint(config);
        String url = config.chatUrl();
        String body = requestBody(config, prompt);
        int timeout = clampTimeout(timeoutSeconds);
        int attempts = Math.max(0, maxRetries) + 1;
        LlmException last = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                return send(config, url, apiKey, body, timeout);
            } catch (LlmException e) {
                last = e;
                if (!e.isRetryable() || attempt == attempts - 1) {
                    throw e;
                }
                long wait = retryBaseMillis * (1L << Math.min(attempt, 8));
                log.warn("大模型调用失败，{} ms 后重试（第 {}/{} 次）endpoint={} reason={}",
                        wait, attempt + 1, attempts - 1, config.endpoint(), e.getReason());
                sleep(wait);
            }
        }
        throw last == null ? new LlmException("LLM_SERVER_ERROR", "大模型调用失败") : last;
    }

    private ChatResult send(LlmConfig config, String url, String apiKey, String body, int timeout) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeout))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(timeout))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (HttpTimeoutException e) {
            throw new LlmException("LLM_TIMEOUT",
                    "大模型调用超时（" + timeout + " 秒），url=" + url, null, true);
        } catch (IOException e) {
            throw new LlmException("LLM_SERVER_ERROR",
                    "大模型网络异常，url=" + url + "：" + safe(e.getMessage()),
                    null, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LlmException("LLM_TIMEOUT", "大模型调用被中断");
        }

        int status = response.statusCode();
        String text = response.body() == null ? "" : response.body();
        if (status == 401 || status == 403) {
            throw new LlmException("LLM_AUTH_FAILED",
                    "大模型鉴权失败，请检查 API Key（HTTP " + status + "，url=" + url + "）",
                    status, false);
        }
        if (status == 429) {
            throw new LlmException("LLM_RATE_LIMITED",
                    "大模型触发限流（HTTP 429，url=" + url + "）：" + snippet(text), status, true);
        }
        if (status >= 500) {
            throw new LlmException("LLM_SERVER_ERROR",
                    "大模型服务异常（HTTP " + status + "，url=" + url + "）：" + snippet(text),
                    status, true);
        }
        if (status >= 400) {
            if (isContextOverflow(text)) {
                throw new LlmException("LLM_ROW_TOO_LONG",
                        "输入超出模型上下文长度（HTTP " + status + "，url=" + url + "）",
                        status, false);
            }
            throw new LlmException("LLM_SERVER_ERROR",
                    "大模型请求被拒绝（HTTP " + status + "，url=" + url + "）：" + snippet(text),
                    status, false);
        }
        return extract(config, text, status);
    }

    private ChatResult extract(LlmConfig config, String text, int status) {
        JsonNode root;
        try {
            root = JsonUtils.mapper().readTree(text);
        } catch (Exception e) {
            throw new LlmException("LLM_PARSE_FAILED",
                    "模型响应不是合法 JSON（HTTP " + status + "）：" + snippet(text), status, false);
        }
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            throw new LlmException("LLM_PARSE_FAILED",
                    "模型响应缺少 choices（HTTP " + status + "）：" + snippet(text), status, false);
        }
        JsonNode content = choices.get(0).path("message").path("content");
        if (content.isMissingNode() || content.isNull()) {
            throw new LlmException("LLM_PARSE_FAILED",
                    "模型响应缺少 message.content：" + snippet(text), status, false);
        }
        JsonNode usage = root.path("usage");
        return new ChatResult(content.asText(),
                usage.path("prompt_tokens").asInt(0),
                usage.path("completion_tokens").asInt(0),
                usage.path("total_tokens").asInt(0),
                status);
    }

    private String requestBody(LlmConfig config, String prompt) {
        ObjectNode root = JsonUtils.mapper().createObjectNode();
        root.put("model", config.model());
        root.put("temperature", 0);
        root.put("stream", false);
        ArrayNode messages = root.putArray("messages");
        ObjectNode message = messages.addObject();
        message.put("role", "user");
        message.put("content", prompt);
        return root.toString();
    }

    /** 保存/运行/预览前统一做 SSRF 校验；解析得到的内网地址默认禁止。 */
    public void checkEndpoint(LlmConfig config) {
        checkUrl(config.chatUrl(), config.nodeId());
    }

    /** 对任意 OpenAI 兼容地址做 SSRF 校验（模型列表接口复用，规则不变）。 */
    public void checkUrl(String url, String nodeId) {
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw endpointError(nodeId, "大模型的 endpoint 不是合法 URL");
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw endpointError(nodeId, "大模型的 endpoint 仅支持 http/https");
        }
        String host = uri.getHost();
        if (host == null || host.trim().isEmpty()) {
            throw endpointError(nodeId, "大模型的 endpoint 缺少主机名");
        }
        AppProperties.Llm llm = appProperties.getLlm();
        if (llm.isAllowPrivateEndpoints()) {
            if (!hostAllowed(host, uri.getPort(), llm.getAllowedHosts())) {
                throw endpointError(nodeId, "endpoint 主机不在 LLM_ALLOWED_HOSTS 白名单内：" + host);
            }
            return;
        }
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (IOException e) {
            throw endpointError(nodeId, "endpoint 主机无法解析：" + host);
        }
        for (InetAddress address : addresses) {
            if (isPrivate(address)) {
                throw endpointError(nodeId,
                        "endpoint 指向内网地址，默认禁止；内网自建模型需开启 LLM_ALLOW_PRIVATE_ENDPOINTS 并配置白名单");
            }
        }
    }

    private static BizException endpointError(String nodeId, String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        if (nodeId != null) {
            data.put("nodeId", nodeId);
        }
        data.put("reason", "LLM_ENDPOINT_INVALID");
        return new BizException(ErrorCode.PARAM_ERROR, message, data);
    }

    /**
     * GET {base}/models：鉴权用该节点已保存的 Key，超时 ≤15s、不重试。
     * 响应与异常绝不出现 Key。
     */
    public List<String> listModels(String modelsUrl, String apiKey, int timeoutSeconds, String nodeId) {
        checkUrl(modelsUrl, nodeId);
        int timeout = Math.max(5, Math.min(15, timeoutSeconds));
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeout))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(modelsUrl))
                .timeout(Duration.ofSeconds(timeout))
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + apiKey)
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (HttpTimeoutException e) {
            throw modelsError(nodeId, null, "模型列表获取超时（" + timeout + " 秒），请检查接口地址");
        } catch (IOException e) {
            throw modelsError(nodeId, null, "模型列表获取失败：网络异常（" + safe(e.getMessage()) + "）");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw modelsError(nodeId, null, "模型列表获取被中断");
        }

        int status = response.statusCode();
        String text = response.body() == null ? "" : response.body();
        if (status == 401 || status == 403) {
            throw modelsError(nodeId, status,
                    "模型列表获取失败：鉴权失败（HTTP " + status + "），请检查 API Key");
        }
        if (status == 404 || status == 405 || status == 501) {
            throw modelsError(nodeId, status, "该地址不支持模型列表，请手动填写模型名称");
        }
        if (status >= 400) {
            throw modelsError(nodeId, status,
                    "模型列表获取失败（HTTP " + status + "）：" + snippet(text));
        }
        List<String> models = parseModels(text);
        if (models.isEmpty()) {
            throw new BizException(ErrorCode.LLM_ERROR, "该地址不支持模型列表，请手动填写模型名称",
                    data(nodeId, "LLM_MODELS_FAILED"));
        }
        return models;
    }

    private static List<String> parseModels(String text) {
        JsonNode root;
        try {
            root = JsonUtils.mapper().readTree(text);
        } catch (Exception e) {
            return List.of();
        }
        Set<String> out = new TreeSet<>();
        JsonNode data = root.path("data");
        if (data.isArray()) {
            for (JsonNode item : data) {
                if (item.isTextual()) {
                    addModel(out, item.asText(""));
                } else if (item.isObject()) {
                    addModel(out, item.path("id").asText(""));
                }
            }
        }
        JsonNode models = root.path("models");
        if (models.isArray()) {
            for (JsonNode item : models) {
                if (item.isTextual()) {
                    addModel(out, item.asText(""));
                } else if (item.isObject()) {
                    addModel(out, item.path("id").asText(item.path("name").asText("")));
                }
            }
        }
        return new ArrayList<>(out);
    }

    private static void addModel(Set<String> models, String name) {
        String value = name == null ? "" : name.trim();
        if (!value.isEmpty()) {
            models.add(value);
        }
    }

    private static BizException modelsError(String nodeId, Integer httpStatus, String message) {
        Map<String, Object> data = data(nodeId, "LLM_MODELS_FAILED");
        if (httpStatus != null) {
            data.put("httpStatus", httpStatus);
        }
        return new BizException(ErrorCode.LLM_ERROR, message, data);
    }

    private static Map<String, Object> data(String nodeId, String reason) {
        Map<String, Object> data = new LinkedHashMap<>();
        if (nodeId != null) {
            data.put("nodeId", nodeId);
        }
        data.put("reason", reason);
        return data;
    }

    private static boolean hostAllowed(String host, int port, String allowedHosts) {
        if (allowedHosts == null || allowedHosts.trim().isEmpty()) {
            return false;
        }
        String lowerHost = host.toLowerCase(Locale.ROOT);
        String hostPort = lowerHost + ":" + port;
        for (String entry : allowedHosts.split(",")) {
            String allowed = entry.trim().toLowerCase(Locale.ROOT);
            if (allowed.isEmpty()) {
                continue;
            }
            if (allowed.equals(lowerHost) || allowed.equals(hostPort)) {
                return true;
            }
        }
        return false;
    }

    static boolean isPrivate(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress()
                || address.isLinkLocalAddress() || address.isSiteLocalAddress()
                || address.isMulticastAddress()) {
            return true;
        }
        byte[] raw = address.getAddress();
        if (raw.length == 4) {
            int first = raw[0] & 0xFF;
            int second = raw[1] & 0xFF;
            // 100.64.0.0/10（CGNAT）
            if (first == 100 && second >= 64 && second <= 127) {
                return true;
            }
            // 0.0.0.0/8
            if (first == 0) {
                return true;
            }
        } else if (raw.length == 16) {
            // fc00::/7（IPv6 唯一本地地址）
            if ((raw[0] & 0xFE) == 0xFC) {
                return true;
            }
        }
        return false;
    }

    private static boolean isContextOverflow(String body) {
        if (body == null) {
            return false;
        }
        String lower = body.toLowerCase(Locale.ROOT);
        return lower.contains("context_length_exceeded") || lower.contains("maximum context length")
                || lower.contains("context length");
    }

    private static int clampTimeout(int seconds) {
        return Math.max(MIN_TIMEOUT, Math.min(MAX_TIMEOUT, seconds));
    }

    private static String snippet(String text) {
        if (text == null) {
            return "";
        }
        String oneLine = text.replace('\n', ' ').trim();
        return oneLine.length() > SNIPPET ? oneLine.substring(0, SNIPPET) + "…" : oneLine;
    }

    private static String safe(String message) {
        if (message == null) {
            return "unknown";
        }
        return message.length() > 200 ? message.substring(0, 200) : message;
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LlmException("LLM_TIMEOUT", "大模型调用被中断");
        }
    }

    public record ChatResult(String content, int promptTokens, int completionTokens,
                             int totalTokens, int httpStatus) {
    }
}
