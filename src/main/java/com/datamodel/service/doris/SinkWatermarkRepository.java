package com.datamodel.service.doris;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.datamodel.domain.SinkWatermarkEntity;
import com.datamodel.mapper.SinkWatermarkMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Sink 水位线读写。失败语义：读/写失败向上抛出（避免静默重复写入）；
 * 删除失败只记日志，不阻断任务删除。
 */
@Repository
public class SinkWatermarkRepository {

    private static final Logger log = LoggerFactory.getLogger(SinkWatermarkRepository.class);

    private final SinkWatermarkMapper mapper;

    public SinkWatermarkRepository(SinkWatermarkMapper mapper) {
        this.mapper = mapper;
    }

    public SinkWatermarkEntity find(Long flowId, String nodeId, String targetKey) {
        return mapper.selectOne(new LambdaQueryWrapper<SinkWatermarkEntity>()
                .eq(SinkWatermarkEntity::getFlowId, flowId)
                .eq(SinkWatermarkEntity::getNodeId, nodeId)
                .eq(SinkWatermarkEntity::getTargetKey, targetKey));
    }

    public List<SinkWatermarkEntity> listByFlow(Long flowId) {
        return mapper.selectList(new LambdaQueryWrapper<SinkWatermarkEntity>()
                .eq(SinkWatermarkEntity::getFlowId, flowId));
    }

    public SinkWatermarkEntity upsert(SinkWatermarkEntity entity) {
        SinkWatermarkEntity existing = find(entity.getFlowId(), entity.getNodeId(), entity.getTargetKey());
        LocalDateTime now = LocalDateTime.now();
        if (existing == null) {
            entity.setCreateTime(now);
            entity.setUpdateTime(now);
            mapper.insert(entity);
            return entity;
        }
        entity.setId(existing.getId());
        entity.setCreateTime(existing.getCreateTime());
        entity.setUpdateTime(now);
        mapper.updateById(entity);
        return entity;
    }

    /** 整任务 FULL 重跑前重置（§19.5）。 */
    public int deleteFlow(Long flowId) {
        try {
            return mapper.delete(new LambdaQueryWrapper<SinkWatermarkEntity>()
                    .eq(SinkWatermarkEntity::getFlowId, flowId));
        } catch (Exception e) {
            log.warn("清理 flow {} 的 Sink 水位线失败：{}", flowId, e.getMessage());
            return 0;
        }
    }

    /** 非复用上游的节点级重跑：该 Sink 重新全量写。 */
    public int deleteNodeTarget(Long flowId, String nodeId, String targetKey) {
        try {
            return mapper.delete(new LambdaQueryWrapper<SinkWatermarkEntity>()
                    .eq(SinkWatermarkEntity::getFlowId, flowId)
                    .eq(SinkWatermarkEntity::getNodeId, nodeId)
                    .eq(SinkWatermarkEntity::getTargetKey, targetKey));
        } catch (Exception e) {
            log.warn("清理 flow {} 节点 {} 的 Sink 水位线失败：{}", flowId, nodeId, e.getMessage());
            return 0;
        }
    }
}
