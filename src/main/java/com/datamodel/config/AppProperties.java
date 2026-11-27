package com.datamodel.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private final Security security = new Security();
    private final SchemaCache schemaCache = new SchemaCache();
    private final DataSourceOptions dataSource = new DataSourceOptions();
    private final Cors cors = new Cors();
    private final Llm llm = new Llm();

    public Security getSecurity() {
        return security;
    }

    public SchemaCache getSchemaCache() {
        return schemaCache;
    }

    public DataSourceOptions getDataSource() {
        return dataSource;
    }

    public Cors getCors() {
        return cors;
    }

    public Llm getLlm() {
        return llm;
    }

    public static class Security {
        /** AES-256-GCM 密钥，32 字节，支持 base64 或 32 字符原文。 */
        private String aesKey;

        public String getAesKey() {
            return aesKey;
        }

        public void setAesKey(String aesKey) {
            this.aesKey = aesKey;
        }
    }

    public static class SchemaCache {
        /** 远端表结构缓存有效期（秒）。 */
        private long ttlSeconds = 300;

        public long getTtlSeconds() {
            return ttlSeconds;
        }

        public void setTtlSeconds(long ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
        }
    }

    public static class DataSourceOptions {
        /** 连接被测数据源时的连接超时（毫秒）。 */
        private int connectTimeoutMs = 5000;
        /** 被测数据源查询超时（秒）。 */
        private int queryTimeoutSeconds = 10;
        /** 节点数据预览查询超时（秒）。 */
        private int previewQueryTimeoutSeconds = 30;

        public int getConnectTimeoutMs() {
            return connectTimeoutMs;
        }

        public void setConnectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
        }

        public int getQueryTimeoutSeconds() {
            return queryTimeoutSeconds;
        }

        public void setQueryTimeoutSeconds(int queryTimeoutSeconds) {
            this.queryTimeoutSeconds = queryTimeoutSeconds;
        }

        public int getPreviewQueryTimeoutSeconds() {
            return previewQueryTimeoutSeconds;
        }

        public void setPreviewQueryTimeoutSeconds(int previewQueryTimeoutSeconds) {
            this.previewQueryTimeoutSeconds = previewQueryTimeoutSeconds;
        }
    }

    public static class Llm {
        /** 是否允许调用私网 endpoint（默认仅公网 https）。 */
        private boolean allowPrivateEndpoints = false;
        /** 允许的 host[:port] 白名单，逗号分隔；allowPrivateEndpoints=true 时命中才放行。 */
        private String allowedHosts = "";
        /** 单次请求输入 JSON 字符数上限，超过自动对半拆批。 */
        private int maxInputChars = 24000;
        /** 失败临时表保留小时数（排查用）。 */
        private int tempTableRetainHours = 24;
        /** 数据预览最多真实调用的行数。 */
        private int previewMaxRows = 20;
        /** 数据预览超时（秒），预览不重试。 */
        private int previewTimeoutSeconds = 30;

        public boolean isAllowPrivateEndpoints() {
            return allowPrivateEndpoints;
        }

        public void setAllowPrivateEndpoints(boolean allowPrivateEndpoints) {
            this.allowPrivateEndpoints = allowPrivateEndpoints;
        }

        public String getAllowedHosts() {
            return allowedHosts;
        }

        public void setAllowedHosts(String allowedHosts) {
            this.allowedHosts = allowedHosts;
        }

        public int getMaxInputChars() {
            return maxInputChars;
        }

        public void setMaxInputChars(int maxInputChars) {
            this.maxInputChars = maxInputChars;
        }

        public int getTempTableRetainHours() {
            return tempTableRetainHours;
        }

        public void setTempTableRetainHours(int tempTableRetainHours) {
            this.tempTableRetainHours = tempTableRetainHours;
        }

        public int getPreviewMaxRows() {
            return previewMaxRows;
        }

        public void setPreviewMaxRows(int previewMaxRows) {
            this.previewMaxRows = previewMaxRows;
        }

        public int getPreviewTimeoutSeconds() {
            return previewTimeoutSeconds;
        }

        public void setPreviewTimeoutSeconds(int previewTimeoutSeconds) {
            this.previewTimeoutSeconds = previewTimeoutSeconds;
        }
    }

    public static class Cors {
        /** 允许的前端来源，逗号分隔。 */
        private String allowedOrigins = "http://localhost:5173,http://127.0.0.1:5173,http://localhost:5175,http://127.0.0.1:5175";

        public String getAllowedOrigins() {
            return allowedOrigins;
        }

        public void setAllowedOrigins(String allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }
    }
}
