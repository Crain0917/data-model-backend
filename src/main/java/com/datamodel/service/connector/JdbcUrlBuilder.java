package com.datamodel.service.connector;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.JsonUtils;
import com.datamodel.config.AppProperties;
import com.datamodel.domain.DataSourceEntity;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Component
public class JdbcUrlBuilder {

    private static final Pattern PARAM_KEY = Pattern.compile("[A-Za-z][A-Za-z0-9_.-]*");

    private final AppProperties appProperties;

    public JdbcUrlBuilder(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    public String build(DataSourceEntity ds) {
        return build(ds, null);
    }

    /**
     * @param queryTimeoutSecondsOverride 非空时覆盖 socketTimeout（预览等长查询场景）
     */
    public String build(DataSourceEntity ds, Integer queryTimeoutSecondsOverride) {
        Map<String, String> extra = parseExtraParams(ds.getExtraParams());
        String type = ds.getType() == null ? "" : ds.getType().toUpperCase();
        switch (type) {
            case "MYSQL":
            case "DORIS":
                return buildMysqlProtocolUrl(ds, extra, queryTimeoutSecondsOverride);
            case "DM8":
                return buildDmUrl(ds, extra);
            default:
                throw new BizException(ErrorCode.PARAM_ERROR, "不支持的数据源类型：" + ds.getType());
        }
    }

    private String buildMysqlProtocolUrl(DataSourceEntity ds, Map<String, String> extra,
                                         Integer queryTimeoutSecondsOverride) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("useUnicode", "true");
        params.put("characterEncoding", "utf8");
        params.put("useSSL", "false");
        params.put("allowPublicKeyRetrieval", "true");
        params.put("serverTimezone", "Asia/Shanghai");
        params.put("connectTimeout", String.valueOf(appProperties.getDataSource().getConnectTimeoutMs()));
        int socketTimeout = queryTimeoutSecondsOverride != null
                ? queryTimeoutSecondsOverride
                : appProperties.getDataSource().getQueryTimeoutSeconds();
        params.put("socketTimeout", String.valueOf(socketTimeout * 1000L));
        // 服务端游标分批读取，避免大表全量加载进内存。
        params.put("useCursorFetch", "true");
        params.put("defaultFetchSize", "1000");
        params.putAll(extra);
        // httpPort 只用于 Stream Load，不是 MySQL 协议参数。
        if ("DORIS".equalsIgnoreCase(ds.getType())) {
            params.remove("httpPort");
        }
        return "jdbc:mysql://" + ds.getHost() + ":" + ds.getPort() + "/" + ds.getDefaultDb()
                + "?" + toQuery(params);
    }

    private String buildDmUrl(DataSourceEntity ds, Map<String, String> extra) {
        Map<String, String> params = new LinkedHashMap<>();
        if (ds.getDefaultDb() != null && !ds.getDefaultDb().trim().isEmpty()) {
            params.put("schema", ds.getDefaultDb());
        }
        params.putAll(extra);
        String suffix = params.isEmpty() ? "" : "?" + toQuery(params);
        return "jdbc:dm://" + ds.getHost() + ":" + ds.getPort() + suffix;
    }

    public Map<String, String> parseExtraParams(String json) {
        if (json == null || json.trim().isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> parsed = JsonUtils.parse(json, new TypeReference<Map<String, Object>>() {
        });
        if (parsed == null) {
            return new LinkedHashMap<>();
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : parsed.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key == null || !PARAM_KEY.matcher(key).matches()) {
                throw new BizException(ErrorCode.PARAM_ERROR, "额外参数键不合法：" + key);
            }
            if (value != null && !(value instanceof String) && !(value instanceof Number)
                    && !(value instanceof Boolean)) {
                throw new BizException(ErrorCode.PARAM_ERROR, "额外参数值仅支持字符串/数字/布尔：" + key);
            }
            result.put(key, value == null ? "" : String.valueOf(value));
        }
        return result;
    }

    private static String toQuery(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (sb.length() > 0) {
                sb.append('&');
            }
            sb.append(entry.getKey()).append('=').append(encode(entry.getValue()));
        }
        return sb.toString();
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value == null ? "" : value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
