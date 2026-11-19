package com.datamodel.service.doris;

import com.datamodel.domain.TableRegistryEntity;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.Locale;

/**
 * 任务源表名生成与复用：同一 flow + 数据源 + 源表只生成一次随机后缀并写入 table_registry，
 * 后续运行、SQL 预览均复用同一物理表名。
 */
@Service
public class SourceTableNameService {

    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
    private static final int SUFFIX_LEN = 6;
    private static final int MAX_ATTEMPTS = 6;

    private final DorisNameSanitizer nameSanitizer;
    private final DorisRegistryRepository registryRepository;
    private final SecureRandom random = new SecureRandom();

    public SourceTableNameService(DorisNameSanitizer nameSanitizer,
                                  DorisRegistryRepository registryRepository) {
        this.nameSanitizer = nameSanitizer;
        this.registryRepository = registryRepository;
    }

    /** 已登记则复用；否则生成并登记。flowId 为空时退回确定性名称（兼容无任务的旧调用）。 */
    public synchronized String resolve(Long flowId, Long dsId, String sourceTable) {
        String table = sourceTable == null ? "" : sourceTable.trim();
        if (flowId == null || dsId == null) {
            return nameSanitizer.sourceTable(dsId == null ? 0L : dsId, table);
        }
        TableRegistryEntity existing = registryRepository.findSrc(flowId, dsId, table);
        if (existing != null && existing.getDorisTable() != null
                && !existing.getDorisTable().trim().isEmpty()) {
            return existing.getDorisTable();
        }
        String randomSuffix = randomSuffix();
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            String candidate = nameSanitizer.flowSourceTable(flowId, table,
                    attempt == 0 ? randomSuffix : randomSuffix());
            TableRegistryEntity entity = new TableRegistryEntity();
            entity.setDorisTable(candidate);
            entity.setKind("SRC");
            entity.setFlowId(flowId);
            entity.setDsId(dsId);
            entity.setSrcTable(table);
            entity.setKeyCols("[\"__dm_rowid\"]");
            entity.setRowCount(0L);
            entity.setByteSize(0L);
            entity.setStatus("READY");
            if (registryRepository.insertIfAbsent(entity)) {
                return candidate;
            }
            // 随机后缀撞车时重试；同时再查一次，避免并发下重复生成。
            TableRegistryEntity raced = registryRepository.findSrc(flowId, dsId, table);
            if (raced != null && raced.getDorisTable() != null) {
                return raced.getDorisTable();
            }
        }
        return deterministic(flowId, dsId, table);
    }

    private String deterministic(Long flowId, Long dsId, String table) {
        String value = flowId + "|" + dsId + "|" + table.toLowerCase(Locale.ROOT);
        return nameSanitizer.flowSourceTable(flowId, table,
                nameSanitizer.stableSuffix(value));
    }

    private String randomSuffix() {
        StringBuilder sb = new StringBuilder(SUFFIX_LEN);
        for (int i = 0; i < SUFFIX_LEN; i++) {
            sb.append(ALPHABET[random.nextInt(ALPHABET.length)]);
        }
        return sb.toString();
    }
}
