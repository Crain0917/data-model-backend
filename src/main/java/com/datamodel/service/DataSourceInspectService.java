package com.datamodel.service;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.crypto.AesGcmCipher;
import com.datamodel.domain.DataSourceEntity;
import com.datamodel.dto.ColumnInfo;
import com.datamodel.dto.TableInfo;
import com.datamodel.dto.TestConnectionResult;
import com.datamodel.service.connector.ConnectionErrorClassifier;
import com.datamodel.service.connector.JdbcConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DataSourceInspectService {

    private static final Logger log = LoggerFactory.getLogger(DataSourceInspectService.class);

    private final DataSourceService dataSourceService;
    private final AesGcmCipher aesGcmCipher;
    private final JdbcConnector jdbcConnector;
    private final SchemaCacheService schemaCacheService;

    public DataSourceInspectService(DataSourceService dataSourceService,
                                    AesGcmCipher aesGcmCipher,
                                    JdbcConnector jdbcConnector,
                                    SchemaCacheService schemaCacheService) {
        this.dataSourceService = dataSourceService;
        this.aesGcmCipher = aesGcmCipher;
        this.jdbcConnector = jdbcConnector;
        this.schemaCacheService = schemaCacheService;
    }

    public TestConnectionResult test(Long id) {
        DataSourceEntity entity = dataSourceService.getEntity(id);
        String password = aesGcmCipher.decrypt(entity.getPasswordEnc());
        TestConnectionResult result = jdbcConnector.test(entity, password);
        dataSourceService.markTestResult(id, result.isSuccess());
        return result;
    }

    public List<TableInfo> listTables(Long id) {
        DataSourceEntity entity = dataSourceService.getEntity(id);
        String password = aesGcmCipher.decrypt(entity.getPasswordEnc());
        try (Connection connection = jdbcConnector.openConnection(entity, password)) {
            return jdbcConnector.listTables(connection, entity);
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw connectionFailure(entity, e);
        }
    }

    public List<ColumnInfo> listColumns(Long id, String table) {
        String safeTable = jdbcConnector.validateTableName(table);
        List<ColumnInfo> cached = schemaCacheService.get(id, safeTable);
        if (cached != null) {
            return cached;
        }
        DataSourceEntity entity = dataSourceService.getEntity(id);
        String password = aesGcmCipher.decrypt(entity.getPasswordEnc());
        try (Connection connection = jdbcConnector.openConnection(entity, password)) {
            List<ColumnInfo> columns = jdbcConnector.listColumns(connection, entity, safeTable);
            schemaCacheService.put(id, entity.getDefaultDb(), safeTable, columns);
            return columns;
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw connectionFailure(entity, e);
        }
    }

    private BizException connectionFailure(DataSourceEntity entity, Exception e) {
        String errorType = ConnectionErrorClassifier.classify(e);
        String message = ConnectionErrorClassifier.messageOf(errorType, e);
        log.info("读取数据源元数据失败 id={}, type={}, errorType={}, msg={}",
                entity.getId(), entity.getType(), errorType, e.getMessage());
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("success", false);
        data.put("errorType", errorType);
        data.put("message", message);
        return new BizException(ErrorCode.CONNECTION_FAILED, message, data);
    }
}
