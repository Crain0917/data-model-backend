package com.datamodel.dto;

import com.datamodel.domain.FlowEntity;

import java.time.LocalDateTime;

public class FlowUpdateResult {

    private Long id;
    private String name;
    private String description;
    private LocalDateTime updateTime;

    public static FlowUpdateResult from(FlowEntity e) {
        FlowUpdateResult result = new FlowUpdateResult();
        result.id = e.getId();
        result.name = e.getName();
        result.description = e.getDescription();
        result.updateTime = e.getUpdateTime();
        return result;
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

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }
}
