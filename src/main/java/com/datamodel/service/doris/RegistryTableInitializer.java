package com.datamodel.service.doris;

import com.datamodel.config.DorisProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 幂等创建 M3 新增的协调元数据表。
 * 只 CREATE TABLE IF NOT EXISTS，不 ALTER / DROP 任何既有表。
 */
@Component
public class RegistryTableInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RegistryTableInitializer.class);

    private static final String REGISTRY_DDL =
            "CREATE TABLE IF NOT EXISTS data_model_table_registry ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                    + "doris_table VARCHAR(64) NOT NULL,"
                    + "kind VARCHAR(8) NOT NULL,"
                    + "flow_id BIGINT NULL,"
                    + "node_id VARCHAR(64) NULL,"
                    + "ds_id BIGINT NULL,"
                    + "src_table VARCHAR(128) NULL,"
                    + "schema_fingerprint VARCHAR(64) NULL,"
                    + "key_cols VARCHAR(255) NULL,"
                    + "row_count BIGINT NOT NULL DEFAULT 0,"
                    + "byte_size BIGINT NOT NULL DEFAULT 0,"
                    + "status VARCHAR(16) NOT NULL DEFAULT 'READY',"
                    + "create_time DATETIME NOT NULL,"
                    + "update_time DATETIME NOT NULL,"
                    + "UNIQUE (doris_table)"
                    + ")";

    private static final String SYNC_LOG_DDL =
            "CREATE TABLE IF NOT EXISTS data_model_sync_log ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                    + "doris_table VARCHAR(64) NOT NULL,"
                    + "label VARCHAR(128) NULL,"
                    + "attempt INT NOT NULL DEFAULT 1,"
                    + "status VARCHAR(16) NOT NULL,"
                    + "row_count BIGINT NOT NULL DEFAULT 0,"
                    + "err_msg TEXT NULL,"
                    + "cost_ms BIGINT NOT NULL DEFAULT 0,"
                    + "create_time DATETIME NOT NULL"
                    + ")";

    private static final String SINK_WATERMARK_DDL =
            "CREATE TABLE IF NOT EXISTS data_model_sink_watermark ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                    + "flow_id BIGINT NOT NULL,"
                    + "node_id VARCHAR(64) NOT NULL,"
                    + "target_key VARCHAR(200) NOT NULL,"
                    + "watermark_column VARCHAR(64) NULL,"
                    + "`last_value` VARCHAR(512) NULL,"
                    + "last_value_type VARCHAR(32) NULL,"
                    + "last_rowid BIGINT NULL,"
                    + "committed_rows BIGINT NOT NULL DEFAULT 0,"
                    + "create_time DATETIME NOT NULL,"
                    + "update_time DATETIME NOT NULL,"
                    + "UNIQUE (flow_id, node_id, target_key)"
                    + ")";

    /** api_key_enc 用 TEXT：4096 字符 Key 的密文约 5.5KB，VARCHAR(1024) 不够。 */
    private static final String LLM_CREDENTIAL_DDL =
            "CREATE TABLE IF NOT EXISTS data_model_llm_credential ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                    + "flow_id BIGINT NOT NULL,"
                    + "node_id VARCHAR(64) NOT NULL,"
                    + "api_key_enc TEXT NOT NULL,"
                    + "key_mask VARCHAR(64) NOT NULL,"
                    + "create_time DATETIME NOT NULL,"
                    + "update_time DATETIME NOT NULL,"
                    + "UNIQUE KEY uk_llm_credential (flow_id, node_id),"
                    + "KEY idx_llm_credential_flow (flow_id)"
                    + ")";

    private final JdbcTemplate jdbcTemplate;
    private final DorisProperties props;

    public RegistryTableInitializer(JdbcTemplate jdbcTemplate, DorisProperties props) {
        this.jdbcTemplate = jdbcTemplate;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!props.isInitRegistryTables()) {
            return;
        }
        createQuietly("data_model_table_registry", REGISTRY_DDL);
        createQuietly("data_model_sync_log", SYNC_LOG_DDL);
        createQuietly("data_model_sink_watermark", SINK_WATERMARK_DDL);
        createQuietly("data_model_llm_credential", LLM_CREDENTIAL_DDL);
    }

    private void createQuietly(String name, String ddl) {
        try {
            jdbcTemplate.execute(ddl);
            log.info("协调元数据表就绪：{}", name);
        } catch (Exception e) {
            log.warn("创建协调元数据表 {} 失败（不影响同步，可运维手工执行 docs/schema-m3.sql）：{}",
                    name, e.getMessage());
        }
    }
}
