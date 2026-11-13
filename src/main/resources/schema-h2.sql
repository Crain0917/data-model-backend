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
