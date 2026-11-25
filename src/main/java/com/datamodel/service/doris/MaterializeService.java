package com.datamodel.service.doris;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.domain.TableRegistryEntity;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.datamodel.sql.Identifiers;
import com.datamodel.sql.OutputColumn;
import com.datamodel.sql.SqlBuildResult;
import com.datamodel.sql.SqlBuilderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 中间节点物化：按需建表 + INSERT OVERWRITE ... SELECT。
 * 表结构未变时复用；结构变化或残留表时 DROP 后重建，CREATE 竞态自动重试一次。
 */
@Service
public class MaterializeService {

    private static final Logger log = LoggerFactory.getLogger(MaterializeService.class);

    private final DorisClient dorisClient;
    private final DorisNameSanitizer nameSanitizer;
    private final SqlBuilderFactory sqlBuilderFactory;
    private final DorisRegistryRepository registryRepository;
    private final TypeMappingRegistry typeMappingRegistry;

    public MaterializeService(DorisClient dorisClient,
                              DorisNameSanitizer nameSanitizer,
                              SqlBuilderFactory sqlBuilderFactory,
                              DorisRegistryRepository registryRepository,
                              TypeMappingRegistry typeMappingRegistry) {
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
        this.sqlBuilderFactory = sqlBuilderFactory;
        this.registryRepository = registryRepository;
        this.typeMappingRegistry = typeMappingRegistry;
    }

    public MaterializeResult materialize(JsonNode dagJson, long flowId, long runId, DagNode node) {
        SqlBuildResult built = sqlBuilderFactory.build(dagJson, flowId, node.id());
        if (built.getSql().trim().startsWith("--")) {
            throw new BizException(ErrorCode.DAG_INVALID, "该节点不产出可执行 SQL：" + node.id(),
                    java.util.Collections.singletonMap("nodeId", node.id()));
        }
        List<OutputColumn> columns = built.getOutputColumns();
        if (columns.isEmpty()) {
            throw new BizException(ErrorCode.DAG_INVALID, "节点配置不完整，无法确定输出字段",
                    java.util.Collections.singletonMap("nodeId", node.id()));
        }
        String target = nameSanitizer.middleTable(flowId, node.id());
        return materializeQuery(flowId, node.id(), target, columns,
                built.getSql(), built.getDisplaySql(), built.getParams());
    }

    /**
     * 把任意 SELECT（可含 WITH）物化到指定中间表：行去重重复行流复用它，
     * 与节点主输出共用建表 / 指纹 / INSERT OVERWRITE 逻辑。
     */
    public MaterializeResult materializeQuery(long flowId, String nodeId, String target,
                                              List<OutputColumn> columns,
                                              String sql, String displaySql,
                                              List<Object> params) {
        List<DdlColumn> ddlColumns = normalize(columns);
        String fingerprint = fingerprint(ddlColumns);

        boolean recreated = prepareTarget(target, fingerprint, ddlColumns);

        String insertSelect = buildInsert(target, sql);
        String displayInsert = buildInsert(target, displaySql);
        execute(insertSelect, displayInsert, params);

        long rows = dorisClient.tableRowCount(target);

        TableRegistryEntity registry = new TableRegistryEntity();
        registry.setDorisTable(target);
        registry.setKind("MID");
        registry.setFlowId(flowId);
        registry.setNodeId(nodeId);
        registry.setSchemaFingerprint(fingerprint);
        registry.setKeyCols("[\"__dm_rowid\"]");
        registry.setRowCount(rows);
        registry.setByteSize(0L);
        registry.setStatus("READY");
        registryRepository.upsert(registry);

        MaterializeResult result = new MaterializeResult();
        result.setTargetTable(target);
        result.setRows(rows);
        result.setSql(displayInsert);
        result.setRecreated(recreated);
        log.info("中间表物化完成 flow={} node={} -> {} 行数={}", flowId, nodeId, target, rows);
        return result;
    }

    /**
     * 表结构未变时保留表直接 INSERT OVERWRITE；结构变化或残留表则 DROP 后重建。
     * information_schema 与物理元数据漂移导致 CREATE 撞「已存在」时，
     * 由 DorisClient.createTable 自动 DROP 并重试一次。
     */
    boolean prepareTarget(String target, String fingerprint, List<DdlColumn> columns) {
        boolean exists = dorisClient.tableExists(target);
        boolean changed = false;
        if (exists) {
            TableRegistryEntity registry = registryRepository.find(target);
            if (registry != null && fingerprint.equals(registry.getSchemaFingerprint())) {
                return false;
            }
            dorisClient.dropTable(target);
            changed = true;
        }
        dorisClient.createTable(buildCreateDdl(target, columns), target);
        return changed;
    }

    private String buildCreateDdl(String target, List<DdlColumn> columns) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(dorisClient.qualified(target)).append(" (\n")
                .append("  `__dm_rowid` BIGINT NOT NULL");
        for (DdlColumn c : columns) {
            ddl.append(",\n  ").append(Identifiers.quote(c.name, Identifiers.QuoteStyle.BACKTICK))
                    .append(" ").append(c.dorisType).append(" NULL");
        }
        ddl.append("\n) DUPLICATE KEY(`__dm_rowid`) DISTRIBUTED BY HASH(`__dm_rowid`) BUCKETS 1 ")
                .append("PROPERTIES (\"replication_num\" = \"1\")");
        return ddl.toString();
    }

    private void execute(String parameterized, String display, List<Object> params) {
        try {
            dorisClient.executePrepared(parameterized, params);
        } catch (BizException e) {
            if (params.isEmpty()) {
                throw e;
            }
            log.warn("参数化 INSERT OVERWRITE 失败，回退展示 SQL 重试一次：{}", e.getMessage());
            dorisClient.execute(display);
        }
    }

    /**
     * CTE 必须提升到 INSERT 之外，否则会被塞进派生表 {@code FROM ( WITH ... SELECT ... )}，
     * Doris 不支持派生表内出现 WITH。
     */
    private String buildInsert(String target, String select) {
        String header = "INSERT OVERWRITE TABLE " + dorisClient.qualifiedQuoted(target) + "\n";
        String trimmed = select.trim();
        int mainSelect = mainSelectAfterCte(trimmed);
        if (mainSelect > 0) {
            String cte = trimmed.substring(0, mainSelect).trim();
            String body = trimmed.substring(mainSelect);
            return header + cte + "\nSELECT row_number() OVER () AS `__dm_rowid`, src.* FROM (\n"
                    + body + "\n) src";
        }
        return header + "SELECT row_number() OVER () AS `__dm_rowid`, src.* FROM (\n"
                + select + "\n) src";
    }

    /** 返回 WITH 子句之后顶层 SELECT 的起始下标；不是 WITH 开头时返回 -1。 */
    private static int mainSelectAfterCte(String sql) {
        if (!sql.regionMatches(true, 0, "WITH ", 0, 5)) {
            return -1;
        }
        int depth = 0;
        boolean inString = false;
        boolean inBacktick = false;
        int n = sql.length();
        for (int i = 4; i < n; i++) {
            char c = sql.charAt(i);
            if (inString) {
                if (c == '\'') {
                    if (i + 1 < n && sql.charAt(i + 1) == '\'') {
                        i++;
                    } else {
                        inString = false;
                    }
                }
                continue;
            }
            if (inBacktick) {
                if (c == '`') {
                    inBacktick = false;
                }
                continue;
            }
            if (c == '\'') {
                inString = true;
            } else if (c == '`') {
                inBacktick = true;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && Character.isWhitespace(c)) {
                int j = i;
                while (j < n && Character.isWhitespace(sql.charAt(j))) {
                    j++;
                }
                if (sql.regionMatches(true, j, "SELECT", 0, 6)
                        && (j + 6 >= n || !Character.isLetterOrDigit(sql.charAt(j + 6)))) {
                    return j;
                }
                i = j - 1;
            }
        }
        return -1;
    }

    private List<DdlColumn> normalize(List<OutputColumn> columns) {
        List<DdlColumn> result = new ArrayList<>();
        Set<String> used = new HashSet<>();
        int index = 0;
        for (OutputColumn c : columns) {
            String name = c.getName() == null ? "" : c.getName().trim();
            if (name.isEmpty()) {
                name = "col_" + index;
            }
            String unique = name;
            int suffix = 2;
            while (!used.add(unique.toLowerCase(Locale.ROOT))) {
                unique = name + "_" + suffix++;
            }
            result.add(new DdlColumn(unique, normalizeType(c.getDorisType())));
            index++;
        }
        return result;
    }

    /** 建表类型一律走类型映射注册表，禁止在此维护第二套白名单。 */
    String normalizeType(String raw) {
        TypeMapping mapping = typeMappingRegistry.normalizeDorisType(raw);
        if (mapping.getWarning() != null) {
            log.warn("中间表字段类型归一化 {} -> {}：{}", raw, mapping.getDorisType(), mapping.getWarning());
        }
        return mapping.getDorisType();
    }

    private static String fingerprint(List<DdlColumn> columns) {
        StringBuilder sb = new StringBuilder();
        for (DdlColumn c : columns) {
            sb.append(c.name).append(':').append(c.dorisType).append(';');
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.substring(0, 16);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static final class DdlColumn {
        final String name;
        final String dorisType;

        DdlColumn(String name, String dorisType) {
            this.name = name;
            this.dorisType = dorisType;
        }
    }
}
