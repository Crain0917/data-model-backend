package com.datamodel.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** 可选携带 dagJson：未传时从 data_model_flow.dag_json 读取。 */
public class SqlPreviewRequest {

    private String dialect = "DORIS";
    private JsonNode dagJson;

    public String getDialect() {
        return dialect;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public JsonNode getDagJson() {
        return dagJson;
    }

    public void setDagJson(JsonNode dagJson) {
        this.dagJson = dagJson;
    }
}
