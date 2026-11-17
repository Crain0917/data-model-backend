package com.datamodel.service.doris;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.datamodel.domain.SyncLogEntity;
import com.datamodel.domain.TableRegistryEntity;
import com.datamodel.mapper.SyncLogMapper;
import com.datamodel.mapper.TableRegistryMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

/**
 * 物化表登记与同步日志。表不存在时降级为只记应用日志，不阻断数据同步。
 */
@Repository
public class DorisRegistryRepository {

    private static final Logger log = LoggerFactory.getLogger(DorisRegistryRepository.class);

    private final TableRegistryMapper registryMapper;
    private final SyncLogMapper syncLogMapper;

    public DorisRegistryRepository(TableRegistryMapper registryMapper, SyncLogMapper syncLogMapper) {
        this.registryMapper = registryMapper;
        this.syncLogMapper = syncLogMapper;
    }

    public TableRegistryEntity find(String dorisTable) {
        try {
            return registryMapper.selectOne(new LambdaQueryWrapper<TableRegistryEntity>()
                    .eq(TableRegistryEntity::getDorisTable, dorisTable));
        } catch (Exception e) {
            log.warn("读取 table_registry 失败（表可能未创建）：{}", e.getMessage());
            return null;
        }
    }

    public void upsert(TableRegistryEntity entity) {
        try {
            TableRegistryEntity existing = find(entity.getDorisTable());
            LocalDateTime now = LocalDateTime.now();
            if (existing == null) {
                entity.setCreateTime(now);
                entity.setUpdateTime(now);
                registryMapper.insert(entity);
            } else {
                entity.setId(existing.getId());
                entity.setCreateTime(existing.getCreateTime());
                entity.setUpdateTime(now);
                registryMapper.updateById(entity);
            }
        } catch (Exception e) {
            log.warn("写 table_registry 失败（表可能未创建）：{}", e.getMessage());
        }
    }

    public void logSync(SyncLogEntity entity) {
        try {
            if (entity.getCreateTime() == null) {
                entity.setCreateTime(LocalDateTime.now());
            }
            syncLogMapper.insert(entity);
        } catch (Exception e) {
            log.warn("写 sync_log 失败（表可能未创建）：{}", e.getMessage());
        }
    }
}
