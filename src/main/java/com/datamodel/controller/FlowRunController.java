package com.datamodel.controller;

import com.datamodel.common.ApiResponse;
import com.datamodel.dto.FlowRunVO;
import com.datamodel.dto.NodeLogsVO;
import com.datamodel.service.FlowRunService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/flow-run")
public class FlowRunController {

    private final FlowRunService flowRunService;

    public FlowRunController(FlowRunService flowRunService) {
        this.flowRunService = flowRunService;
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
}
