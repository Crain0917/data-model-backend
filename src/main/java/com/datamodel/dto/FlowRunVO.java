package com.datamodel.dto;

import com.datamodel.domain.FlowRunEntity;

import java.time.LocalDateTime;
import java.util.List;

public class FlowRunVO {

    private Long runId;
    private Long flowId;
    private String flowName;
    private String runType;
    private String targetNodeId;
    private String status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long costMs;
    private String errMsg;
    private List<NodeRunVO> nodes;

    public static FlowRunVO from(FlowRunEntity e) {
        FlowRunVO vo = new FlowRunVO();
        vo.runId = e.getId();
        vo.flowId = e.getFlowId();
        vo.flowName = e.getFlowName();
        vo.runType = e.getRunType();
        vo.targetNodeId = e.getTargetNode();
        vo.status = e.getStatus();
        vo.startTime = e.getStartTime();
        vo.endTime = e.getEndTime();
        vo.costMs = e.getCostMs();
        vo.errMsg = e.getErrMsg();
        return vo;
    }

    public Long getRunId() {
        return runId;
    }

    public Long getFlowId() {
        return flowId;
    }

    public String getFlowName() {
        return flowName;
    }

    public String getRunType() {
        return runType;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public String getStatus() {
        return status;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public Long getCostMs() {
        return costMs;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public List<NodeRunVO> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeRunVO> nodes) {
        this.nodes = nodes;
    }
}
