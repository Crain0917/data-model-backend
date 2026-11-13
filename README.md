# 可视化数据集成平台 · 后端（data-model-backend）

Spring Boot 单体后端。当前版本完成 **M1 数据源域**：数据源 CRUD、连通性测试、表/字段元数据查询；执行引擎、DAG、SQL 编排、WebSocket 在后续里程碑实现。

- 元数据库：MySQL 8+（生产使用服务器 `data_model` 库）
- 计算库：Doris（MySQL 协议，默认 9030）
- 支持的数据源：`MYSQL`、`DORIS`、`DM8`

## 1. 技术栈与依赖

| 依赖 | 版本 | 说明 |
|---|---|---|
| JDK | 1.8 | 编译与运行 |
| Spring Boot | 2.7.18 | Web / Validation |
| MyBatis-Plus | 3.5.5 | 元数据 CRUD |
| MySQL Connector/J | 8.0.33 | MySQL、Doris |
| DmJdbcDriver18 | 8.1.3.62 | 达梦 DM8 |
| H2 | 2.2.224 | 仅 `local` 配置自测 |
| Maven | 3.6+ | 构建 |

本机 Maven 仓库可通过 `settings.xml` 指向内网仓库；项目本身不依赖私有包。

## 2. 目录结构

```
data-model-backend/
├── pom.xml
├── README.md
└── src/
    ├── main/java/com/datamodel/
    │   ├── DataModelApplication.java
    │   ├── common/          # 统一响应、错误码、异常、AES-GCM、JSON 工具
    │   ├── config/          # AppProperties、MyBatis-Plus、Jackson、CORS
    │   ├── controller/      # DataSourceController
    │   ├── domain/          # DataSourceEntity、SchemaCacheEntity
    │   ├── dto/             # 请求/响应对象
    │   ├── mapper/          # MyBatis-Plus Mapper
    │   ├── service/         # DataSourceService、InspectService、SchemaCacheService
    │   │   └── connector/   # 驱动解析、URL 构建、错误分类、JDBC 元数据读取
    │   ├── engine/          # 执行引擎占位（M3–M4）
    │   └── sql/             # SQL 构建占位（M3–M4）
    └── main/resources/
        ├── application.yml
        ├── application-local.yml   # H2 自测
        └── schema-h2.sql           # 仅 H2 自测建表
    └── test/java/com/datamodel/
        ├── AesGcmCipherTest.java
        ├── JdbcUrlBuilderTest.java
        ├── ConnectionErrorClassifierTest.java
        ├── JdbcMetadataReaderTest.java
        └── DataSourceApiIntegrationTest.java
```

## 3. 环境变量

应用**不读取任何硬编码账号密码**，全部由环境变量注入。

| 变量 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `AES_KEY` | 是 | 无 | 密码加密密钥。32 字节，支持 Base64 或 32 字符原文 |
| `DB_HOST` | 是 | `127.0.0.1` | 元数据库主机 |
| `DB_PORT` | 否 | `3306` | 元数据库端口 |
| `DB_NAME` | 否 | `data_model` | 元数据库库名 |
| `DB_USER` | 是 | 无 | 元数据库账号 |
| `DB_PASSWORD` | 是 | 无 | 元数据库密码 |
| `SERVER_PORT` | 否 | `8080` | 服务端口 |
| `DB_POOL_MAX` | 否 | `10` | Hikari 最大连接数 |
| `DB_POOL_MIN` | 否 | `2` | Hikari 最小空闲连接数 |
| `SCHEMA_CACHE_TTL_SECONDS` | 否 | `300` | 元数据缓存 TTL（秒） |
| `DS_CONNECT_TIMEOUT_MS` | 否 | `5000` | 被测数据源连接超时（毫秒） |
| `DS_QUERY_TIMEOUT_SECONDS` | 否 | `10` | 被测数据源查询超时（秒） |
| `CORS_ALLOWED_ORIGINS` | 否 | `http://localhost:5173,http://127.0.0.1:5173` | 前端来源白名单，逗号分隔 |

生成密钥：

```bash
# Linux / macOS / Git Bash
export AES_KEY="$(openssl rand -base64 32)"
```

```powershell
# Windows PowerShell
$bytes = New-Object byte[] 32
[Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
$env:AES_KEY = [Convert]::ToBase64String($bytes)
```

> `AES_KEY` 必须妥善保管且保持稳定。更换密钥后，已保存的数据源密码将无法解密，需要重新录入密码。

## 4. 构建与启动

```bash
mvn clean package
```

产物：`target/data-model-backend.jar`

启动（示例，密码由 shell 环境提供，不写进命令历史或文档）：

```bash
export DB_HOST=192.168.10.11
export DB_PORT=3306
export DB_NAME=data_model
export DB_USER=data_model
export DB_PASSWORD='<元数据库密码>'
export AES_KEY='<32 字节密钥>'
java -jar target/data-model-backend.jar
```

Windows CMD：

```bat
set DB_HOST=192.168.10.11
set DB_PORT=3306
set DB_NAME=data_model
set DB_USER=data_model
set DB_PASSWORD=<元数据库密码>
set AES_KEY=<32 字节密钥>
java -jar target\data-model-backend.jar
```

缺少 `AES_KEY` 或长度不合法时，应用启动即失败并给出明确提示（安全默认）。

### 离线自测（H2，不连服务器）

```bash
export AES_KEY="$(openssl rand -base64 32)"
java -jar target/data-model-backend.jar --spring.profiles.active=local
```

`local` 配置使用内存 H2（MySQL 兼容模式），自带 `schema-h2.sql`，仅用于接口自测。

## 5. 数据库说明

元数据库表由运维/架构预置，应用**不自动建表**（`spring.sql.init.mode=never`）。设计基线见 `项目管理/架构/schema.sql`。

当前服务器 `data_model` 库已存在 7 张表。与设计文档存在字段命名差异，后端已按**现有物理表**适配：

| 用途 | 物理表 | 物理字段 | API 字段 |
|---|---|---|---|
| 数据源 | `data_model_data_source` | `default_db` | `dbName` |
| 数据源 | `data_model_data_source` | `status`（`UNKNOWN` / `OK` / `FAIL`） | `lastTestStatus`（`null` / `SUCCESS` / `FAILED`） |
| 元数据缓存 | `data_model_schema_cache` | `fields_json`、`update_time` | 内部使用；`update_time` 作为缓存 TTL 基准 |

> 其余表（流程定义、执行记录等）当前未使用。
>
> **M1 评审口径（已定）**：现有物理表是 v1 环境事实源，不修改共享表结构、不执行 `schema.sql` 的 ALTER/DROP；后端按物理列适配，API 保持契约字段。7 张表的完整列↔字段映射、索引差异与 M4 前置项见 [`docs/物理表映射.md`](docs/物理表映射.md)。

## 6. 接口

统一前缀 `/api/data-source`，统一响应体：

```json
{ "code": 0, "data": {}, "msg": "" }
```

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/data-source?page=1&size=10&keyword=&type=` | 分页查询，`keyword` 匹配名称 |
| POST | `/api/data-source` | 新增 |
| PUT | `/api/data-source/{id}` | 更新；`password` 为空表示保留原密码 |
| DELETE | `/api/data-source/{id}` | 删除并清理元数据缓存 |
| POST | `/api/data-source/{id}/test` | 测试连接（JDBC `SELECT 1`） |
| GET | `/api/data-source/{id}/tables` | 表列表 |
| GET | `/api/data-source/{id}/tables/{table}/columns` | 字段列表 |

业务错误码：

| code | HTTP | 含义 |
|---|---|---|
| 0 | 200 | 成功 |
| 4000 | 400 | 参数错误 |
| 4002 | 404 | 资源不存在 |
| 4003 | 409 | 名称等唯一约束冲突 |
| 4004 | 400 | 数据源连通失败（含 `errorType`） |
| 5000 | 500 | 内部错误（不返回堆栈） |
| 5001 | 500 | Doris 执行失败 |
| 5002 | 500 | 驱动/解密/连接错误 |

未定义路由统一返回 HTTP 404 + `code=4002`；请求方法不支持返回 HTTP 405 + `code=4000`；均不返回 500 堆栈。

连通失败时 `data` 结构：

```json
{
  "success": false,
  "latencyMs": 5012,
  "errorType": "REFUSED",
  "message": "连接被拒绝，请检查主机、端口与数据库服务"
}
```

`errorType` 取值：`REFUSED`、`AUTH_FAIL`、`DB_NOT_FOUND`、`TIMEOUT`、`DRIVER_MISSING`、`OTHER`。

## 7. curl 验证

以下命令中的数据库密码通过环境变量 `DS_PASSWORD` 传入，请勿直接写进命令或文件。

```bash
BASE=http://127.0.0.1:8080
export DS_PASSWORD='<被测数据库密码>'
```

### 7.1 新增 MySQL 数据源

```bash
curl -s -X POST "$BASE/api/data-source" \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"ods_mysql",
    "type":"MYSQL",
    "host":"192.168.10.11",
    "port":3306,
    "dbName":"data_model",
    "username":"data_model",
    "password":"'"$DS_PASSWORD"'",
    "remark":"本地验证"
  }'
```

返回 `data.id` 即为数据源 ID。

### 7.2 列表

```bash
curl -s "$BASE/api/data-source?page=1&size=10&keyword=ods_mysql"
```

### 7.3 测试连接

```bash
curl -s -X POST "$BASE/api/data-source/1/test"
```

成功返回 `{"code":0,"data":{"success":true,"latencyMs":12,...}}`。
不可达时返回 HTTP 400、`code=4004`，而不是 500 堆栈。

### 7.4 表列表

```bash
curl -s "$BASE/api/data-source/1/tables"
```

### 7.5 字段列表

```bash
curl -s "$BASE/api/data-source/1/tables/data_model_data_source/columns"
```

### 7.6 更新（空密码保留原密码）

```bash
curl -s -X PUT "$BASE/api/data-source/1" \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"ods_mysql",
    "type":"MYSQL",
    "host":"192.168.10.11",
    "port":3306,
    "dbName":"data_model",
    "username":"data_model",
    "password":"",
    "remark":"更新备注"
  }'
```

### 7.7 删除

```bash
curl -s -X DELETE "$BASE/api/data-source/1"
```

### 7.8 Doris 数据源

将 `type` 改为 `DORIS`、`port` 改为 `9030`，其余同上：

```bash
curl -s -X POST "$BASE/api/data-source" \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"ods_doris",
    "type":"DORIS",
    "host":"192.168.10.11",
    "port":9030,
    "dbName":"data_model",
    "username":"data_model",
    "password":"'"$DS_PASSWORD"'"
  }'
```

Doris 表/字段分别使用 `SHOW TABLES`、`SHOW COLUMNS`。

## 8. 安全设计

- 数据源密码 **AES-256-GCM** 加密后存储：随机 12 字节 IV、128-bit tag，密文格式 `v1:<iv_base64>:<ciphertext+tag_base64>`。
- 密码为只写字段：新增/更新接收，查询、详情、测试连接等任何响应均不返回；日志不打印密码与 SQL 参数。
- 数据库连接信息只来自环境变量，代码、配置、README 中不出现真实凭据。
- 表名参数使用白名单校验（`[A-Za-z0-9_$]+`），JDBC URL 附加参数仅允许受限键名与标量值，降低注入风险。
- 统一异常处理不向客户端返回堆栈。
- CORS 仅放行配置的前端来源。

## 9. 测试与验证结果

```bash
mvn test
```

27 个用例全部通过：AES-GCM 加解密/篡改检测、JDBC URL 构建与参数转义、错误分类、H2 元数据读取、MockMvc 接口测试（CRUD、密码不回显、空密码保留、4002/4003/4004/4000、删除清缓存、未定义路由 404、错误方法 405）。

已在服务器环境完成真实链路验证：

- MySQL 元数据库测试连接成功（约 10–15ms），7 张表，字段读取正常
- Doris 测试连接成功（约 17–91ms），表 `orders` / `orders_bak` / `users`，字段读取正常
- 不可达地址返回 HTTP 400 + `code=4004`，`errorType=REFUSED`，无 500 堆栈
- 库中 `password_enc` 为 `v1:` 前缀密文，响应与运行日志均无明文密码
- 删除数据源后元数据缓存同步清理

## 10. 已知问题与未完成项

1. **演示数据 `doris_demo` 已通过接口重存密码**（2026-11-13）：换用新的 `AES_KEY` 后原密文无法解密，已用 `PUT /api/data-source/1` 重新加密保存，现测试连接、表/字段查询均正常。**`AES_KEY` 必须保持稳定**，再次更换需按同样方式重存所有数据源密码。
2. **DM8 未连真实实例验证**：驱动已打包，元数据读取使用 `DatabaseMetaData` 实现；需达梦测试库补验。
3. **物理表与设计 DDL 不一致**：详见第 5 节，待架构师统一。
4. 执行引擎、DAG 校验、SQL 构建、任务调度、WebSocket 推送尚未实现（M3–M5）。
5. 登录鉴权、操作审计未实现（v1 规划默认无登录）。

## 11. 后续接口占位

`engine/`、`sql/` 包已预留，M3–M4 将实现：

- 流程定义/画布保存与 DAG 校验（4001）
- 单节点执行与预览、执行记录（4005 / 4006 / 5001）
- MySQL / Doris / DM8 到 Doris 的 SQL 生成与调度
