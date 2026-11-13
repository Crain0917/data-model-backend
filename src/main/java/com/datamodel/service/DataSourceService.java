package com.datamodel.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.PageResult;
import com.datamodel.common.crypto.AesGcmCipher;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.dto.DataSourceQuery;
import com.datamodel.dto.DataSourceRequest;
import com.datamodel.dto.DataSourceVO;
import com.datamodel.mapper.DataSourceMapper;
import com.datamodel.service.connector.JdbcUrlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class DataSourceService {

    private static final Logger log = LoggerFactory.getLogger(DataSourceService.class);

    private final DataSourceMapper dataSourceMapper;
    private final AesGcmCipher aesGcmCipher;
    private final SchemaCacheService schemaCacheService;
    private final JdbcUrlBuilder jdbcUrlBuilder;

    public DataSourceService(DataSourceMapper dataSourceMapper,
                             AesGcmCipher aesGcmCipher,
                             SchemaCacheService schemaCacheService,
                             JdbcUrlBuilder jdbcUrlBuilder) {
        this.dataSourceMapper = dataSourceMapper;
        this.aesGcmCipher = aesGcmCipher;
        this.schemaCacheService = schemaCacheService;
        this.jdbcUrlBuilder = jdbcUrlBuilder;
    }

    public PageResult<DataSourceVO> list(DataSourceQuery query) {
        int page = query.getPage() == null || query.getPage() < 1 ? 1 : query.getPage();
        int size = query.getSize() == null || query.getSize() < 1 ? 20 : Math.min(query.getSize(), 200);

        LambdaQueryWrapper<DataSourceEntity> wrapper = new LambdaQueryWrapper<>();
        if (StringUtils.hasText(query.getKeyword())) {
            wrapper.like(DataSourceEntity::getName, query.getKeyword().trim());
        }
        if (StringUtils.hasText(query.getType())) {
            wrapper.eq(DataSourceEntity::getType, query.getType().trim().toUpperCase());
        }
        wrapper.orderByDesc(DataSourceEntity::getId);

        Page<DataSourceEntity> result = dataSourceMapper.selectPage(new Page<>(page, size), wrapper);
        List<DataSourceVO> list = result.getRecords().stream()
                .map(DataSourceVO::from)
                .collect(Collectors.toList());
        return PageResult.of(list, result.getTotal());
    }

    @Transactional
    public Long create(DataSourceRequest request) {
        String name = trim(request.getName());
        if (!StringUtils.hasText(request.getPassword())) {
            throw new BizException(ErrorCode.PARAM_ERROR, "密码不能为空");
        }
        ensureNameAvailable(name, null);
        jdbcUrlBuilder.parseExtraParams(request.getExtraParams());

        LocalDateTime now = LocalDateTime.now();
        DataSourceEntity entity = new DataSourceEntity();
        entity.setName(name);
        entity.setType(request.getType().trim().toUpperCase());
        entity.setHost(trim(request.getHost()));
        entity.setPort(request.getPort());
        entity.setDefaultDb(trim(request.getDbName()));
        entity.setUsername(trim(request.getUsername()));
        entity.setPasswordEnc(aesGcmCipher.encrypt(request.getPassword()));
        entity.setExtraParams(trimToNull(request.getExtraParams()));
        entity.setRemark(trimToNull(request.getRemark()));
        entity.setStatus("UNKNOWN");
        entity.setCreateTime(now);
        entity.setUpdateTime(now);
        try {
            dataSourceMapper.insert(entity);
        } catch (DuplicateKeyException e) {
            throw new BizException(ErrorCode.CONFLICT, "数据源名称已存在");
        }
        log.info("新增数据源 id={}, name={}, type={}", entity.getId(), entity.getName(), entity.getType());
        return entity.getId();
    }

    @Transactional
    public void update(Long id, DataSourceRequest request) {
        DataSourceEntity entity = getEntity(id);
        String name = trim(request.getName());
        ensureNameAvailable(name, id);
        jdbcUrlBuilder.parseExtraParams(request.getExtraParams());

        String newType = request.getType().trim().toUpperCase();
        String newHost = trim(request.getHost());
        String newDb = trim(request.getDbName());
        String newUser = trim(request.getUsername());
        boolean passwordChanged = StringUtils.hasText(request.getPassword());

        boolean connectionChanged = !Objects.equals(entity.getType(), newType)
                || !Objects.equals(entity.getHost(), newHost)
                || !Objects.equals(entity.getPort(), request.getPort())
                || !Objects.equals(entity.getDefaultDb(), newDb)
                || !Objects.equals(entity.getUsername(), newUser)
                || passwordChanged;

        entity.setName(name);
        entity.setType(newType);
        entity.setHost(newHost);
        entity.setPort(request.getPort());
        entity.setDefaultDb(newDb);
        entity.setUsername(newUser);
        if (passwordChanged) {
            entity.setPasswordEnc(aesGcmCipher.encrypt(request.getPassword()));
        }
        entity.setExtraParams(trimToNull(request.getExtraParams()));
        entity.setRemark(trimToNull(request.getRemark()));
        if (connectionChanged) {
            entity.setStatus("UNKNOWN");
            entity.setLastTestTime(null);
        }
        entity.setUpdateTime(LocalDateTime.now());
        try {
            dataSourceMapper.updateById(entity);
        } catch (DuplicateKeyException e) {
            throw new BizException(ErrorCode.CONFLICT, "数据源名称已存在");
        }
        schemaCacheService.evict(id);
        log.info("更新数据源 id={}, connectionChanged={}", id, connectionChanged);
    }

    @Transactional
    public void delete(Long id) {
        DataSourceEntity entity = getEntity(id);
        dataSourceMapper.deleteById(id);
        schemaCacheService.evict(id);
        log.info("删除数据源 id={}, name={}", entity.getId(), entity.getName());
    }

    public DataSourceEntity getEntity(Long id) {
        if (id == null) {
            throw new BizException(ErrorCode.PARAM_ERROR, "数据源 ID 不能为空");
        }
        DataSourceEntity entity = dataSourceMapper.selectById(id);
        if (entity == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "数据源不存在");
        }
        return entity;
    }

    public void markTestResult(Long id, boolean success) {
        dataSourceMapper.update(null, new LambdaUpdateWrapper<DataSourceEntity>()
                .eq(DataSourceEntity::getId, id)
                .set(DataSourceEntity::getStatus, success ? "OK" : "FAIL")
                .set(DataSourceEntity::getLastTestTime, LocalDateTime.now())
                .set(DataSourceEntity::getUpdateTime, LocalDateTime.now()));
    }

    private void ensureNameAvailable(String name, Long excludeId) {
        LambdaQueryWrapper<DataSourceEntity> wrapper = new LambdaQueryWrapper<DataSourceEntity>()
                .eq(DataSourceEntity::getName, name);
        if (excludeId != null) {
            wrapper.ne(DataSourceEntity::getId, excludeId);
        }
        Long count = dataSourceMapper.selectCount(wrapper);
        if (count != null && count > 0) {
            throw new BizException(ErrorCode.CONFLICT, "数据源名称已存在");
        }
    }

    private static String trim(String value) {
        return value == null ? null : value.trim();
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
