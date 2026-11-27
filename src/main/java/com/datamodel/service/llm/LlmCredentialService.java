package com.datamodel.service.llm;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.datamodel.common.BizException;
import com.datamodel.common.ErrorCode;
import com.datamodel.common.crypto.AesGcmCipher;
import com.datamodel.domain.LlmCredentialEntity;
import com.datamodel.mapper.FlowMapper;
import com.datamodel.mapper.LlmCredentialMapper;
import com.datamodel.dto.LlmKeyStatus;
import com.datamodel.service.doris.DorisClient;
import com.datamodel.service.doris.DorisNameSanitizer;
import com.datamodel.sql.Dag;
import com.datamodel.sql.DagNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LLM 节点 API Key 的读写、掩码、生命周期清理。
 * 明文只在执行/预览内存中出现；日志、异常与响应只允许出现 mask。
 */
@Service
public class LlmCredentialService {

    private static final Logger log = LoggerFactory.getLogger(LlmCredentialService.class);
    private static final int MAX_KEY_LENGTH = 4096;

    private final LlmCredentialMapper credentialMapper;
    private final FlowMapper flowMapper;
    private final AesGcmCipher cipher;
    private final DorisClient dorisClient;
    private final DorisNameSanitizer nameSanitizer;

    public LlmCredentialService(LlmCredentialMapper credentialMapper, FlowMapper flowMapper,
                                AesGcmCipher cipher, DorisClient dorisClient,
                                DorisNameSanitizer nameSanitizer) {
        this.credentialMapper = credentialMapper;
        this.flowMapper = flowMapper;
        this.cipher = cipher;
        this.dorisClient = dorisClient;
        this.nameSanitizer = nameSanitizer;
    }

    public LlmKeyStatus status(long flowId, String nodeId) {
        requireFlow(flowId);
        LlmCredentialEntity entity = find(flowId, nodeId);
        return entity == null ? new LlmKeyStatus(false, null)
                : new LlmKeyStatus(true, entity.getKeyMask());
    }

    public LlmKeyStatus put(long flowId, String nodeId, String apiKey) {
        requireFlow(flowId);
        String key = apiKey == null ? "" : apiKey.trim();
        if (key.isEmpty()) {
            throw keyError("API Key 不能为空");
        }
        if (key.length() > MAX_KEY_LENGTH) {
            throw keyError("API Key 长度不能超过 " + MAX_KEY_LENGTH + " 个字符");
        }
        String mask = mask(key);
        LocalDateTime now = LocalDateTime.now();
        LlmCredentialEntity entity = find(flowId, nodeId);
        if (entity == null) {
            entity = new LlmCredentialEntity();
            entity.setFlowId(flowId);
            entity.setNodeId(nodeId);
            entity.setApiKeyEnc(cipher.encrypt(key));
            entity.setKeyMask(mask);
            entity.setCreateTime(now);
            entity.setUpdateTime(now);
            credentialMapper.insert(entity);
        } else {
            entity.setApiKeyEnc(cipher.encrypt(key));
            entity.setKeyMask(mask);
            entity.setUpdateTime(now);
            credentialMapper.updateById(entity);
        }
        log.info("设置大模型 Key flow={} node={} mask={}", flowId, nodeId, mask);
        return new LlmKeyStatus(true, mask);
    }

    public LlmKeyStatus delete(long flowId, String nodeId) {
        requireFlow(flowId);
        LlmCredentialEntity entity = find(flowId, nodeId);
        if (entity != null) {
            credentialMapper.deleteById(entity.getId());
            log.info("删除大模型 Key flow={} node={}", flowId, nodeId);
        }
        return new LlmKeyStatus(false, null);
    }

    /** 执行/预览用：无 Key 返回 null；解密失败只提示重新设置，不泄露任何密文。 */
    public String plainKey(long flowId, String nodeId) {
        LlmCredentialEntity entity = find(flowId, nodeId);
        if (entity == null || entity.getApiKeyEnc() == null || entity.getApiKeyEnc().isEmpty()) {
            return null;
        }
        try {
            return cipher.decrypt(entity.getApiKeyEnc());
        } catch (RuntimeException e) {
            log.warn("大模型 Key 解密失败 flow={} node={}", flowId, nodeId);
            throw new BizException(ErrorCode.INTERNAL_ERROR,
                    "大模型 API Key 解密失败，请重新设置密钥",
                    Map.of("nodeId", nodeId, "reason", "LLM_KEY_DECRYPT_FAILED"));
        }
    }

    public String maskOf(long flowId, String nodeId) {
        LlmCredentialEntity entity = find(flowId, nodeId);
        return entity == null ? null : entity.getKeyMask();
    }

    /**
     * 保存画布对账：删除已不存在的 LLM 节点的 Key 与 dm_llm_* 临时表。
     * Doris 不可达只 WARN，不阻断保存。
     */
    public void reconcile(long flowId, JsonNode dagJson) {
        Set<String> keep = new LinkedHashSet<>();
        Dag dag;
        try {
            dag = Dag.parse(dagJson);
        } catch (RuntimeException e) {
            log.warn("大模型 Key 对账跳过 flow={}：DAG 无法解析：{}", flowId, e.getMessage());
            return;
        }
        for (DagNode node : dag.nodeList()) {
            if ("llm".equals(node.type())) {
                keep.add(node.id());
            }
        }
        List<LlmCredentialEntity> rows;
        try {
            rows = credentialMapper.selectList(new LambdaQueryWrapper<LlmCredentialEntity>()
                    .eq(LlmCredentialEntity::getFlowId, flowId));
        } catch (RuntimeException e) {
            log.warn("读取大模型 Key 失败 flow={}：{}", flowId, e.getMessage());
            return;
        }
        Set<String> removed = new LinkedHashSet<>();
        for (LlmCredentialEntity row : rows) {
            if (keep.contains(row.getNodeId())) {
                continue;
            }
            try {
                credentialMapper.deleteById(row.getId());
                removed.add(row.getNodeId());
                log.info("保存画布清理已删除 LLM 节点的 Key flow={} node={}", flowId, row.getNodeId());
            } catch (RuntimeException e) {
                log.warn("清理大模型 Key 失败 flow={} node={}：{}", flowId, row.getNodeId(), e.getMessage());
            }
        }
        for (String nodeId : removed) {
            dropMapTable(flowId, nodeId);
        }
    }

    /** 流程删除：清理本 flow 的全部 Key。 */
    public void deleteFlow(long flowId) {
        try {
            credentialMapper.delete(new LambdaQueryWrapper<LlmCredentialEntity>()
                    .eq(LlmCredentialEntity::getFlowId, flowId));
        } catch (RuntimeException e) {
            log.warn("清理大模型 Key 失败 flow={}：{}", flowId, e.getMessage());
        }
    }

    private void dropMapTable(long flowId, String nodeId) {
        String table = nameSanitizer.llmMapTable(flowId, nodeId);
        try {
            dorisClient.dropTable(table);
        } catch (RuntimeException e) {
            log.warn("清理大模型临时表失败 flow={} table={}：{}", flowId, table, e.getMessage());
        }
    }

    private LlmCredentialEntity find(long flowId, String nodeId) {
        return credentialMapper.selectOne(new LambdaQueryWrapper<LlmCredentialEntity>()
                .eq(LlmCredentialEntity::getFlowId, flowId)
                .eq(LlmCredentialEntity::getNodeId, nodeId)
                .last("LIMIT 1"));
    }

    private void requireFlow(long flowId) {
        if (flowMapper.selectById(flowId) == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "任务不存在");
        }
    }

    private static BizException keyError(String message) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("reason", "LLM_KEY_EMPTY");
        return new BizException(ErrorCode.PARAM_ERROR, message, data);
    }

    static String mask(String key) {
        if (key == null) {
            return "****";
        }
        if (key.length() <= 8) {
            return "****";
        }
        return key.substring(0, 4) + "****" + key.substring(key.length() - 4);
    }
}
