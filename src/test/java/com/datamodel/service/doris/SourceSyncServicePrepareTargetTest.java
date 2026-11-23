package com.datamodel.service.doris;

import com.datamodel.config.DorisProperties;
import com.datamodel.domain.TableRegistryEntity;
import com.datamodel.service.connector.JdbcConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SourceSyncServicePrepareTargetTest {

    private static final String TABLE = "dm_src_109_qa_orders_abc123";
    private static final String FINGERPRINT = "ce857ad149629f17";

    private DorisClient dorisClient;
    private DorisRegistryRepository registryRepository;
    private SourceSyncService service;

    @BeforeEach
    void setUp() {
        dorisClient = mock(DorisClient.class);
        registryRepository = mock(DorisRegistryRepository.class);
        when(dorisClient.qualified(TABLE)).thenReturn("data_model." + TABLE);
        service = new SourceSyncService(mock(DorisProperties.class), dorisClient,
                mock(JdbcConnector.class), mock(TypeMappingRegistry.class),
                mock(DorisNameSanitizer.class), registryRepository,
                mock(SourceTableNameService.class));
    }

    @Test
    void alwaysDropsResidualTableBeforeRecreateWhenSchemaUnchanged() {
        when(dorisClient.tableExists(TABLE)).thenReturn(true);
        TableRegistryEntity registry = new TableRegistryEntity();
        registry.setSchemaFingerprint(FINGERPRINT);
        when(registryRepository.find(TABLE)).thenReturn(registry);

        boolean changed = service.prepareTarget(TABLE, FINGERPRINT, columns());

        assertFalse(changed);
        verify(dorisClient).dropTable(TABLE);
        verify(dorisClient).createTable(anyString(), eq(TABLE));
    }

    @Test
    void alwaysDropsEvenWhenInformationSchemaSaysMissing() {
        when(dorisClient.tableExists(TABLE)).thenReturn(false);

        boolean changed = service.prepareTarget(TABLE, FINGERPRINT, columns());

        assertFalse(changed);
        verify(registryRepository, never()).find(anyString());
        verify(dorisClient).dropTable(TABLE);
        verify(dorisClient).createTable(anyString(), eq(TABLE));
    }

    @Test
    void reportsRecreatedWhenFingerprintChanged() {
        when(dorisClient.tableExists(TABLE)).thenReturn(true);
        TableRegistryEntity registry = new TableRegistryEntity();
        registry.setSchemaFingerprint("old-fingerprint");
        when(registryRepository.find(TABLE)).thenReturn(registry);

        assertTrue(service.prepareTarget(TABLE, FINGERPRINT, columns()));
        verify(dorisClient).dropTable(TABLE);
        verify(dorisClient).createTable(anyString(), eq(TABLE));
    }

    private static List<SourceSyncService.TargetColumn> columns() {
        return Collections.singletonList(new SourceSyncService.TargetColumn(
                "order_id", "order_id", "BIGINT", null));
    }
}
