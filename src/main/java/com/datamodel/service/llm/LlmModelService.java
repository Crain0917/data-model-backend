package com.datamodel.service.llm;

import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.dto.LlmModelsResponse;
import com.datamodel.service.FlowService;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 大模型模型列表：使用该节点已保存的 Key 拉 GET {base}/models。
 * endpoint 优先取查询参数，缺省回退到该节点已保存配置；Key 不出现在响应或日志。
 */
@Service
public class LlmModelService {

    private final LlmCredentialService credentialService;
    private final LlmClient llmClient;
    private final FlowService flowService;

    public LlmModelService(LlmCredentialService credentialService, LlmClient llmClient,
                           FlowService flowService) {
        this.credentialService = credentialService;
        this.llmClient = llmClient;
        this.flowService = flowService;
    }

    public LlmModelsResponse list(long flowId, String nodeId, String endpointParam) {
        String apiKey = credentialService.plainKey(flowId, nodeId);
        if (apiKey == null || apiKey.isEmpty()) {
            throw new BizException(ErrorCode.LLM_ERROR,
                    "该节点尚未保存 API Key，请先保存 Key 再获取模型列表",
                    data(nodeId, "LLM_KEY_MISSING"));
        }
        String endpoint = endpointParam == null ? "" : endpointParam.trim();
        if (endpoint.isEmpty()) {
            endpoint = savedEndpoint(flowId, nodeId);
        }
        if (endpoint.isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "请先填写模型接口地址",
                    data(nodeId, "LLM_ENDPOINT_EMPTY"));
        }
        String modelsUrl = LlmConfig.normalizeModelsUrl(endpoint);
        List<String> models = llmClient.listModels(modelsUrl, apiKey, 15, nodeId);
        return new LlmModelsResponse(models);
    }

    private String savedEndpoint(long flowId, String nodeId) {
        JsonNode dagJson = flowService.parseDag(flowService.getEntity(flowId));
        if (dagJson == null) {
            return "";
        }
        DagNode node = Dag.parse(dagJson).node(nodeId);
        return node == null ? "" : node.config().path("endpoint").asText("").trim();
    }

    private static Map<String, Object> data(String nodeId, String reason) {
        Map<String, Object> data = new LinkedHashMap<>();
        if (nodeId != null) {
            data.put("nodeId", nodeId);
        }
        data.put("reason", reason);
        return data;
    }
}
