package com.datamodel.dto;

import java.util.List;

/** 模型列表：去重排序后的模型名称。 */
public class LlmModelsResponse {

    private List<String> models;

    public LlmModelsResponse() {
    }

    public LlmModelsResponse(List<String> models) {
        this.models = models;
    }

    public List<String> getModels() {
        return models;
    }

    public void setModels(List<String> models) {
        this.models = models;
    }
}
