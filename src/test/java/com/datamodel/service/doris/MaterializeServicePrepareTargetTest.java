package com.datamodel.service.doris;

import com.datamodel.domain.TableRegistryEntity;
import com.datamodel.sql.SqlBuilderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MaterializeServicePrepareTargetTest {

    private static final String TABLE = "dm_mid_109_node_join_x";
    private static final String FINGERPRINT = "640dd57fe85cfc4e";

    private DorisClient dorisClient;
    private DorisRegistryRepository registryRepository;
    private MaterializeService service;

    @BeforeEach
    void setUp() {
        dorisClient = mock(DorisClient.class);
        registryRepository = mock(DorisRegistryRepository.class);
        when(dorisClient.qualified(TABLE)).thenReturn("data_model." + TABLE);
        service = new MaterializeService(dorisClient, mock(DorisNameSanitizer.class),
                mock(SqlBuilderFactory.class), registryRepository, mock(TypeMappingRegistry.class));
    }

    @Test
    void keepsTableWhenSchemaUnchanged() {
        when(dorisClient.tableExists(TABLE)).thenReturn(true);
        TableRegistryEntity registry = new TableRegistryEntity();
        registry.setSchemaFingerprint(FINGERPRINT);
        when(registryRepository.find(TABLE)).thenReturn(registry);

        assertFalse(service.prepareTarget(TABLE, FINGERPRINT, columns()));
        verify(dorisClient, never()).dropTable(anyString());
        verify(dorisClient, never()).createTable(anyString(), anyString());
    }

    @Test
    void dropsAndRecreatesWhenSchemaChanged() {
        when(dorisClient.tableExists(TABLE)).thenReturn(true);
        TableRegistryEntity registry = new TableRegistryEntity();
        registry.setSchemaFingerprint("old-fingerprint");
        when(registryRepository.find(TABLE)).thenReturn(registry);

        assertTrue(service.prepareTarget(TABLE, FINGERPRINT, columns()));
        verify(dorisClient).dropTable(TABLE);
        verify(dorisClient).createTable(anyString(), eq(TABLE));
    }

    @Test
    void createsWhenInformationSchemaSaysMissingButRetriesOnExistingRace() {
        when(dorisClient.tableExists(TABLE)).thenReturn(false);

        assertFalse(service.prepareTarget(TABLE, FINGERPRINT, columns()));
        verify(dorisClient, never()).dropTable(anyString());
        verify(dorisClient).createTable(anyString(), eq(TABLE));
    }

    private static List<MaterializeService.DdlColumn> columns() {
        return Collections.singletonList(new MaterializeService.DdlColumn("order_id", "BIGINT"));
    }
}
