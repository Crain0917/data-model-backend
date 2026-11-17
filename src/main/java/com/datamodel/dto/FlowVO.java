package com.datamodel.dto;

import com.datamodel.domain.FlowEntity;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;

public class FlowVO {

    private Long id;
    private String name;
    private String description;
    private Integer nodeCount;
    private String lastRunStatus;
    private LocalDateTime lastRunTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private JsonNode dagJson;

    public static FlowVO summary(FlowEntity e) {
        FlowVO vo = new FlowVO();
        vo.id = e.getId();
        vo.name = e.getName();
        vo.description = e.getDescription();
        vo.nodeCount = e.getNodeCount();
        vo.lastRunStatus = e.getLastRunStatus();
        vo.lastRunTime = e.getLastRunTime();
        vo.createTime = e.getCreateTime();
        vo.updateTime = e.getUpdateTime();
        return vo;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Integer getNodeCount() {
        return nodeCount;
    }

    public String getLastRunStatus() {
        return lastRunStatus;
    }

    public LocalDateTime getLastRunTime() {
        return lastRunTime;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public JsonNode getDagJson() {
        return dagJson;
    }

    public void setDagJson(JsonNode dagJson) {
        this.dagJson = dagJson;
    }
}
