package com.datamodel.controller;

import com.datamodel.common.ApiResponse;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.PageResult;
import com.datamodel.dto.ColumnListResult;
import com.datamodel.dto.DataSourceQuery;
import com.datamodel.dto.DataSourceRequest;
import com.datamodel.dto.DataSourceVO;
import com.datamodel.dto.IdResult;
import com.datamodel.dto.TableListResult;
import com.datamodel.dto.TestConnectionResult;
import com.datamodel.service.DataSourceInspectService;
import com.datamodel.service.DataSourceService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/data-source")
public class DataSourceController {

    private final DataSourceService dataSourceService;
    private final DataSourceInspectService inspectService;

    public DataSourceController(DataSourceService dataSourceService,
                                DataSourceInspectService inspectService) {
        this.dataSourceService = dataSourceService;
        this.inspectService = inspectService;
    }

    @GetMapping
    public ApiResponse<PageResult<DataSourceVO>> list(@ModelAttribute DataSourceQuery query) {
        return ApiResponse.ok(dataSourceService.list(query));
    }

    @PostMapping
    public ApiResponse<IdResult> create(@Valid @RequestBody DataSourceRequest request) {
        return ApiResponse.ok(new IdResult(dataSourceService.create(request)));
    }

    @PutMapping("/{id}")
    public ApiResponse<Void> update(@PathVariable Long id, @Valid @RequestBody DataSourceRequest request) {
        dataSourceService.update(id, request);
        return ApiResponse.ok();
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        dataSourceService.delete(id);
        return ApiResponse.ok();
    }

    @PostMapping("/{id}/test")
    public ResponseEntity<ApiResponse<TestConnectionResult>> test(@PathVariable Long id) {
        TestConnectionResult result = inspectService.test(id);
        if (result.isSuccess()) {
            return ResponseEntity.ok(ApiResponse.ok(result));
        }
        return ResponseEntity.status(ErrorCode.CONNECTION_FAILED.getHttpStatus())
                .body(ApiResponse.fail(ErrorCode.CONNECTION_FAILED, result.getMessage(), result));
    }

    @GetMapping("/{id}/tables")
    public ApiResponse<TableListResult> tables(@PathVariable Long id) {
        return ApiResponse.ok(new TableListResult(inspectService.listTables(id)));
    }

    @GetMapping("/{id}/tables/{table}/columns")
    public ApiResponse<ColumnListResult> columns(@PathVariable Long id, @PathVariable String table) {
        return ApiResponse.ok(new ColumnListResult(inspectService.listColumns(id, table)));
    }
}
