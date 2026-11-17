package com.datamodel.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.time.LocalDateTime;

/** Sink 写入水位线；每批提交后推进（架构 M3.7 §19）。 */
@TableName("data_model_sink_watermark")
public class SinkWatermarkEntity {

    @TableId(type = IdType.AUTO)
    private Long id;
    private Long flowId;
    private String nodeId;
    private String targetKey;
    private String watermarkColumn;
    /** LAST_VALUE 是 MySQL 8 窗口函数保留字，必须显式引用。 */
    @TableField("`last_value`")
    private String lastValue;
    private String lastValueType;
    private Long lastRowid;
    private Long committedRows;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getFlowId() {
        return flowId;
    }

    public void setFlowId(Long flowId) {
        this.flowId = flowId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public void setTargetKey(String targetKey) {
        this.targetKey = targetKey;
    }

    public String getWatermarkColumn() {
        return watermarkColumn;
    }

    public void setWatermarkColumn(String watermarkColumn) {
        this.watermarkColumn = watermarkColumn;
    }

    public String getLastValue() {
        return lastValue;
    }

    public void setLastValue(String lastValue) {
        this.lastValue = lastValue;
    }

    public String getLastValueType() {
        return lastValueType;
    }

    public void setLastValueType(String lastValueType) {
        this.lastValueType = lastValueType;
    }

    public Long getLastRowid() {
        return lastRowid;
    }

    public void setLastRowid(Long lastRowid) {
        this.lastRowid = lastRowid;
    }

    public Long getCommittedRows() {
        return committedRows;
    }

    public void setCommittedRows(Long committedRows) {
        this.committedRows = committedRows;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }
}
