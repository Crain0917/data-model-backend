package com.datamodel.dto;

/** M3.7 §21.1：仅更新任务名称与描述，不动画布与执行状态。 */
public class FlowUpdateRequest {

    private String name;
    private String description;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
