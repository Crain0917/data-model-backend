package com.datamodel.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Doris 计算层连接与同步参数。账号密码只从环境变量注入，不进仓库。
 */
@ConfigurationProperties(prefix = "app.doris")
public class DorisProperties {

    /** FE 主机。 */
    private String host = "127.0.0.1";
    /** Stream Load HTTP 端口。 */
    private int httpPort = 8040;
    /** MySQL 协议查询端口。 */
    private int queryPort = 9030;
    private String user;
    private String password;
    /** 物化表统一落的 schema。 */
    private String schema = "data_model";
    /** 单批行数上限。 */
    private int batchRows = 100000;
    /** 单批字节上限。 */
    private long batchBytes = 100L * 1024 * 1024;
    /** Stream Load 单次超时（秒）。 */
    private int timeoutSeconds = 600;
    /** 单表软上限（字节），超限拒绝导入。 */
    private long maxTargetTableBytes = 2L * 1024 * 1024 * 1024;
    /** 单次重试次数（指数退避）。 */
    private int retryTimes = 2;
    /** 自动创建协调元数据表（table_registry / sync_log）。 */
    private boolean initRegistryTables = true;

    public boolean isConfigured() {
        // Doris 允许空密码（如内网 root），用户名是必填项。
        return user != null && !user.trim().isEmpty();
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public void setQueryPort(int queryPort) {
        this.queryPort = queryPort;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public int getBatchRows() {
        return batchRows;
    }

    public void setBatchRows(int batchRows) {
        this.batchRows = batchRows;
    }

    public long getBatchBytes() {
        return batchBytes;
    }

    public void setBatchBytes(long batchBytes) {
        this.batchBytes = batchBytes;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public long getMaxTargetTableBytes() {
        return maxTargetTableBytes;
    }

    public void setMaxTargetTableBytes(long maxTargetTableBytes) {
        this.maxTargetTableBytes = maxTargetTableBytes;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public boolean isInitRegistryTables() {
        return initRegistryTables;
    }

    public void setInitRegistryTables(boolean initRegistryTables) {
        this.initRegistryTables = initRegistryTables;
    }
}
