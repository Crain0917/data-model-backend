package com.datamodel.service.doris;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

class DorisClientCreateTableTest {

    private static final String DDL = "CREATE TABLE data_model.t1 (`__dm_rowid` BIGINT NOT NULL)";
    private static final String TABLE = "t1";

    @Test
    void retriesAfterDroppingWhenCreateHitsAlreadyExists() {
        DorisClient client = mock(DorisClient.class, withSettings().defaultAnswer(
                org.mockito.Answers.CALLS_REAL_METHODS));
        doThrow(alreadyExists())
                .doNothing()
                .when(client).execute(DDL);
        doNothing().when(client).dropTable(TABLE);

        client.createTable(DDL, TABLE);

        verify(client, times(2)).execute(DDL);
        verify(client).dropTable(TABLE);
    }

    @Test
    void doesNotDropOnOtherErrors() {
        DorisClient client = mock(DorisClient.class, withSettings().defaultAnswer(
                org.mockito.Answers.CALLS_REAL_METHODS));
        doThrow(new BizException(ErrorCode.DORIS_EXEC_FAILED, "Doris 执行失败：Unknown column 'x'"))
                .when(client).execute(DDL);

        assertThrows(BizException.class, () -> client.createTable(DDL, TABLE));

        verify(client, never()).dropTable(anyString());
        verify(client, times(1)).execute(DDL);
    }

    private static BizException alreadyExists() {
        return new BizException(ErrorCode.DORIS_EXEC_FAILED,
                "Doris 执行失败：errCode = 2, detailMessage = Table [data_model.t1] already exists");
    }
}
