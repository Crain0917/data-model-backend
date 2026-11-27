-- =====================================================================
-- 本地自测用 H2(MySQL 模式) 表结构，字段与服务器现有 MySQL 表一致。
-- 生产/联调请直接使用服务器 MySQL（schema: data_model）。
-- =====================================================================

CREATE TABLE IF NOT EXISTS data_model_data_source (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY,
  name           VARCHAR(128) NOT NULL,
  type           VARCHAR(32)  NOT NULL,
  host           VARCHAR(128) NOT NULL,
  port           INT          NOT NULL,
  default_db     VARCHAR(128),
  username       VARCHAR(128) NOT NULL,
  password_enc   CLOB,
  extra_params   CLOB,
  remark         VARCHAR(512),
  status         VARCHAR(16)  NOT NULL DEFAULT 'UNKNOWN',
  last_test_time DATETIME,
  create_time    DATETIME     NOT NULL,
  update_time    DATETIME     NOT NULL,
  CONSTRAINT uk_data_source_name UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS data_model_schema_cache (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY,
  data_source_id BIGINT       NOT NULL,
  db_name        VARCHAR(128),
  table_name     VARCHAR(128) NOT NULL,
  fields_json    CLOB,
  create_time    DATETIME     NOT NULL,
  update_time    DATETIME     NOT NULL,
  CONSTRAINT uk_schema_cache UNIQUE (data_source_id, table_name)
);

CREATE TABLE IF NOT EXISTS data_model_sink_watermark (
  id               BIGINT AUTO_INCREMENT PRIMARY KEY,
  flow_id          BIGINT       NOT NULL,
  node_id          VARCHAR(64)  NOT NULL,
  target_key       VARCHAR(200) NOT NULL,
  watermark_column VARCHAR(64),
  last_value       VARCHAR(512),
  last_value_type  VARCHAR(32),
  last_rowid       BIGINT,
  committed_rows   BIGINT       NOT NULL DEFAULT 0,
  create_time      DATETIME     NOT NULL,
  update_time      DATETIME     NOT NULL,
  CONSTRAINT uk_sink_watermark UNIQUE (flow_id, node_id, target_key)
);

CREATE TABLE IF NOT EXISTS data_model_llm_credential (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  flow_id     BIGINT      NOT NULL,
  node_id     VARCHAR(64) NOT NULL,
  api_key_enc CLOB        NOT NULL,
  key_mask    VARCHAR(64) NOT NULL,
  create_time DATETIME    NOT NULL,
  update_time DATETIME    NOT NULL,
  CONSTRAINT uk_llm_credential UNIQUE (flow_id, node_id)
);
