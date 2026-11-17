package com.datamodel.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** 节点数据预览请求：dagJson 缺省读已保存画布，limit 缺省 100、硬上限 200。 */
public class NodeDataPreviewRequest {

    private JsonNode dagJson;
    private Integer limit;

    public JsonNode getDagJson() {
        return dagJson;
    }

    public void setDagJson(JsonNode dagJson) {
        this.dagJson = dagJson;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }
}
