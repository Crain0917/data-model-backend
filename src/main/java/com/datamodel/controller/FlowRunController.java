package com.datamodel.controller;

import com.datamodel.common.ApiResponse;
import com.datamodel.dto.FlowRunVO;
import com.datamodel.dto.NodeErrorDetailVO;
import com.datamodel.dto.NodeLogsVO;
import com.datamodel.service.FlowRunService;
import com.datamodel.service.NodeErrorDetailService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/flow-run")
public class FlowRunController {

    private final FlowRunService flowRunService;
    private final NodeErrorDetailService nodeErrorDetailService;

    public FlowRunController(FlowRunService flowRunService,
                             NodeErrorDetailService nodeErrorDetailService) {
        this.flowRunService = flowRunService;
        this.nodeErrorDetailService = nodeErrorDetailService;
    }

    @GetMapping("/{runId}")
    public ApiResponse<FlowRunVO> detail(@PathVariable Long runId) {
        return ApiResponse.ok(flowRunService.getRun(runId));
    }

    @GetMapping("/{runId}/node/{nodeId}/logs")
    public ApiResponse<NodeLogsVO> nodeLogs(@PathVariable Long runId,
                                            @PathVariable String nodeId) {
        return ApiResponse.ok(flowRunService.nodeLogs(runId, nodeId));
    }

    @GetMapping("/{runId}/node/{nodeId}/error-detail")
    public ApiResponse<NodeErrorDetailVO> nodeErrorDetail(@PathVariable Long runId,
                                                          @PathVariable String nodeId) {
        return ApiResponse.ok(nodeErrorDetailService.detail(runId, nodeId));
    }
}
