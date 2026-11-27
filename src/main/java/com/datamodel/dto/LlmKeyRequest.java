package com.datamodel.dto;

/** PUT /api/flow/{flowId}/node/{nodeId}/llm-key 请求体；仅 apiKey，绝不出现在 dagJson。 */
public class LlmKeyRequest {

    private String apiKey;

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }
}
