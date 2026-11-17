package com.datamodel.dto;

import com.fasterxml.jackson.databind.JsonNode;

public class FlowDagRequest {

    private JsonNode dagJson;

    public JsonNode getDagJson() {
        return dagJson;
    }

    public void setDagJson(JsonNode dagJson) {
        this.dagJson = dagJson;
    }
}
