package com.datamodel.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private final Security security = new Security();
    private final SchemaCache schemaCache = new SchemaCache();
    private final DataSourceOptions dataSource = new DataSourceOptions();
    private final Cors cors = new Cors();

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

    public static class Cors {
        /** 允许的前端来源，逗号分隔。 */
        private String allowedOrigins = "http://localhost:5173,http://127.0.0.1:5173";

        public String getAllowedOrigins() {
            return allowedOrigins;
        }

        public void setAllowedOrigins(String allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }
    }
}
