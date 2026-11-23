package com.datamodel.controller;

import com.datamodel.common.ApiResponse;
import com.datamodel.common.PageResult;
import com.datamodel.dto.FlowCreateRequest;
import com.datamodel.dto.FlowDagRequest;
import com.datamodel.dto.FlowDeleteResult;
import com.datamodel.dto.FlowRunVO;
import com.datamodel.dto.FlowUpdateRequest;
import com.datamodel.dto.FlowUpdateResult;
import com.datamodel.dto.FlowVO;
import com.datamodel.dto.IdResult;
import com.datamodel.dto.NodeDataPreviewRequest;
import com.datamodel.dto.NodeDataPreviewResponse;
import com.datamodel.dto.SqlPreviewRequest;
import com.datamodel.dto.SqlPreviewResponse;
import com.datamodel.service.FlowRunService;
import com.datamodel.service.FlowService;
import com.datamodel.service.NodeDataService;
import com.datamodel.service.NodeSqlService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import java.util.Collections;

@RestController
@RequestMapping("/api/flow")
public class FlowController {

    private final FlowService flowService;
    private final NodeSqlService nodeSqlService;
    private final NodeDataService nodeDataService;
    private final FlowRunService flowRunService;

    public FlowController(FlowService flowService, NodeSqlService nodeSqlService,
                          NodeDataService nodeDataService, FlowRunService flowRunService) {
        this.flowService = flowService;
        this.nodeSqlService = nodeSqlService;
        this.nodeDataService = nodeDataService;
        this.flowRunService = flowRunService;
    }

    @GetMapping
    public ApiResponse<PageResult<FlowVO>> list(
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size) {
        return ApiResponse.ok(flowService.list(keyword, page, size));
    }

    @PostMapping
    public ApiResponse<IdResult> create(@Valid @RequestBody FlowCreateRequest request) {
        return ApiResponse.ok(new IdResult(flowService.create(request)));
    }

    @GetMapping("/{id}")
    public ApiResponse<FlowVO> detail(@PathVariable Long id) {
        return ApiResponse.ok(flowService.detail(id));
    }

    @PutMapping("/{id}/dag")
    public ApiResponse<Void> saveDag(@PathVariable Long id, @RequestBody FlowDagRequest request) {
        flowService.saveDag(id, request.getDagJson());
        return ApiResponse.ok();
    }

    /** M3.7 §21.1：只更新任务名称/描述，不动画布。 */
    @PutMapping("/{id}")
    public ApiResponse<FlowUpdateResult> update(@PathVariable Long id,
                                                @RequestBody FlowUpdateRequest request) {
        return ApiResponse.ok(flowService.update(id, request));
    }

    /** M3.7 §21.2：confirmName 必须与任务名 trim 后精确匹配。 */
    @DeleteMapping("/{id}")
    public ApiResponse<FlowDeleteResult> delete(
            @PathVariable Long id,
            @RequestParam(value = "confirmName", required = false) String confirmName) {
        FlowDeleteResult result = flowService.delete(id, confirmName);
        if (result.getFailedTables().isEmpty()) {
            return ApiResponse.ok(result);
        }
        return new ApiResponse<>(0, result,
                "任务已删除，但 " + result.getFailedTables().size() + " 张节点表删除失败，已记录日志");
    }

    @PostMapping("/{id}/run")
    public ResponseEntity<ApiResponse<Object>> run(@PathVariable Long id) {
        Long runId = flowRunService.startFlowRun(id);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(ApiResponse.ok(Collections.singletonMap("runId", runId)));
    }

    /** 中间任务：不重新同步 source，只重跑其余节点；前置校验源表副本。 */
    @PostMapping("/{id}/run-middle")
    public ResponseEntity<ApiResponse<Object>> runMiddle(@PathVariable Long id) {
        Long runId = flowRunService.startMiddleRun(id);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(ApiResponse.ok(Collections.singletonMap("runId", runId)));
    }

    @PostMapping("/{id}/node/{nodeId}/run")
    public ResponseEntity<ApiResponse<Object>> runNode(
            @PathVariable Long id,
            @PathVariable String nodeId,
            @RequestParam(value = "reuseUpstream", required = false, defaultValue = "false")
                    boolean reuseUpstream) {
        Long runId = flowRunService.startNodeRun(id, nodeId, reuseUpstream);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(ApiResponse.ok(Collections.singletonMap("runId", runId)));
    }

    @PostMapping("/{id}/node/{nodeId}/sql")
    public ApiResponse<SqlPreviewResponse> previewSql(@PathVariable Long id,
                                                      @PathVariable String nodeId,
                                                      @RequestBody(required = false) SqlPreviewRequest request) {
        return ApiResponse.ok(nodeSqlService.preview(id, nodeId, request));
    }

    /** M3.6 §10：节点数据预览，只读；Source 过滤下推源库。 */
    @PostMapping("/{id}/node/{nodeId}/data")
    public ApiResponse<NodeDataPreviewResponse> previewData(
            @PathVariable Long id,
            @PathVariable String nodeId,
            @RequestBody(required = false) NodeDataPreviewRequest request) {
        return ApiResponse.ok(nodeDataService.preview(id, nodeId, request));
    }

    @GetMapping("/{id}/runs")
    public ApiResponse<PageResult<FlowRunVO>> runs(
            @PathVariable Long id,
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size) {
        return ApiResponse.ok(flowRunService.listRuns(id, page, size));
    }
}
