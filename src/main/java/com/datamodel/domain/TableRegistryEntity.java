package com.datamodel.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.time.LocalDateTime;

/** 物化表登记（Doris 表名唯一事实源）。 */
@TableName("data_model_table_registry")
public class TableRegistryEntity {

    @TableId(type = IdType.AUTO)
    private Long id;
    private String dorisTable;
    private String kind;
    private Long flowId;
    private String nodeId;
    private Long dsId;
    private String srcTable;
    private String schemaFingerprint;
    private String keyCols;
    private Long rowCount;
    private Long byteSize;
    private String status;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDorisTable() {
        return dorisTable;
    }

    public void setDorisTable(String dorisTable) {
        this.dorisTable = dorisTable;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
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

    public Long getDsId() {
        return dsId;
    }

    public void setDsId(Long dsId) {
        this.dsId = dsId;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getSchemaFingerprint() {
        return schemaFingerprint;
    }

    public void setSchemaFingerprint(String schemaFingerprint) {
        this.schemaFingerprint = schemaFingerprint;
    }

    public String getKeyCols() {
        return keyCols;
    }

    public void setKeyCols(String keyCols) {
        this.keyCols = keyCols;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public void setRowCount(Long rowCount) {
        this.rowCount = rowCount;
    }

    public Long getByteSize() {
        return byteSize;
    }

    public void setByteSize(Long byteSize) {
        this.byteSize = byteSize;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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
