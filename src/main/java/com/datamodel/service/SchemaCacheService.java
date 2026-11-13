package com.datamodel.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.datamodel.common.JsonUtils;
import com.datamodel.config.AppProperties;
import com.datamodel.domain.SchemaCacheEntity;
import com.datamodel.dto.ColumnInfo;
import com.datamodel.mapper.SchemaCacheMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class SchemaCacheService {

    private final SchemaCacheMapper schemaCacheMapper;
    private final AppProperties appProperties;

    public SchemaCacheService(SchemaCacheMapper schemaCacheMapper, AppProperties appProperties) {
        this.schemaCacheMapper = schemaCacheMapper;
        this.appProperties = appProperties;
    }

    public List<ColumnInfo> get(Long dataSourceId, String tableName) {
        SchemaCacheEntity entity = find(dataSourceId, tableName);
        if (entity == null || entity.getUpdateTime() == null || entity.getFieldsJson() == null) {
            return null;
        }
        long ttl = appProperties.getSchemaCache().getTtlSeconds();
        if (entity.getUpdateTime().plusSeconds(ttl).isBefore(LocalDateTime.now())) {
            return null;
        }
        return JsonUtils.parse(entity.getFieldsJson(), new TypeReference<List<ColumnInfo>>() {
        });
    }

    public void put(Long dataSourceId, String dbName, String tableName, List<ColumnInfo> columns) {
        LocalDateTime now = LocalDateTime.now();
        SchemaCacheEntity existing = find(dataSourceId, tableName);
        if (existing == null) {
            SchemaCacheEntity entity = new SchemaCacheEntity();
            entity.setDataSourceId(dataSourceId);
            entity.setDbName(dbName);
            entity.setTableName(tableName);
            entity.setFieldsJson(JsonUtils.toJson(columns));
            entity.setCreateTime(now);
            entity.setUpdateTime(now);
            schemaCacheMapper.insert(entity);
        } else {
            existing.setDbName(dbName);
            existing.setFieldsJson(JsonUtils.toJson(columns));
            existing.setUpdateTime(now);
            schemaCacheMapper.updateById(existing);
        }
    }

    public void evict(Long dataSourceId) {
        schemaCacheMapper.delete(new LambdaQueryWrapper<SchemaCacheEntity>()
                .eq(SchemaCacheEntity::getDataSourceId, dataSourceId));
    }

    private SchemaCacheEntity find(Long dataSourceId, String tableName) {
        return schemaCacheMapper.selectOne(new LambdaQueryWrapper<SchemaCacheEntity>()
                .eq(SchemaCacheEntity::getDataSourceId, dataSourceId)
                .eq(SchemaCacheEntity::getTableName, tableName));
    }
}
