package com.datamodel.sink;

import java.util.ArrayList;
import java.util.List;

/** Sink 节点配置（架构 M3.7 §17）。字段名与 dag_json 一致。 */
public class SinkConfig {

    public static final String DORIS = "DORIS";
    public static final String APPEND = "APPEND";
    public static final String OVERWRITE = "OVERWRITE";
    public static final String UNIQUE = "UNIQUE";
    public static final String DUPLICATE = "DUPLICATE";
    public static final int DEFAULT_BATCH_SIZE = 10000;
    public static final int MIN_BATCH_SIZE = 100;
    public static final int MAX_BATCH_SIZE = 100000;

    private String nodeId;
    private String targetType = DORIS;
    private Long dataSourceId;
    private String tableName;
    private String writeMode = APPEND;
    /** Doris 建表模型；缺省 DUPLICATE，兼容未配置该字段的旧 DAG。 */
    private String tableModel = DUPLICATE;
    /** tableModel 是否由用户显式配置（用于区分旧 DAG 与显式 Duplicate + 覆盖）。 */
    private boolean tableModelExplicit;
    /** UNIQUE 模型的主键目标列，顺序即建表键顺序。 */
    private List<String> keyColumns = new ArrayList<>();
    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean autoCreateTable = true;
    private String watermarkColumn;
    private List<SinkField> fieldMapping = new ArrayList<>();
    /** 运行时目标标识（Doris 含数据源名/host/端口/库），用于日志与节点详情。 */
    private String targetLabelOverride;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public Long getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(Long dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getTableModel() {
        return tableModel;
    }

    public void setTableModel(String tableModel) {
        this.tableModel = tableModel == null ? DUPLICATE : tableModel;
    }

    public boolean isTableModelExplicit() {
        return tableModelExplicit;
    }

    public void setTableModelExplicit(boolean tableModelExplicit) {
        this.tableModelExplicit = tableModelExplicit;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(List<String> keyColumns) {
        this.keyColumns = keyColumns == null ? new ArrayList<>() : keyColumns;
    }

    public boolean isUniqueModel() {
        return UNIQUE.equalsIgnoreCase(tableModel);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isAutoCreateTable() {
        return autoCreateTable;
    }

    public void setAutoCreateTable(boolean autoCreateTable) {
        this.autoCreateTable = autoCreateTable;
    }

    public String getWatermarkColumn() {
        return watermarkColumn;
    }

    public void setWatermarkColumn(String watermarkColumn) {
        this.watermarkColumn = watermarkColumn;
    }

    public List<SinkField> getFieldMapping() {
        return fieldMapping;
    }

    public void setFieldMapping(List<SinkField> fieldMapping) {
        this.fieldMapping = fieldMapping == null ? new ArrayList<>() : fieldMapping;
    }

    public boolean isDoris() {
        return DORIS.equals(targetType);
    }

    public boolean isOverwrite() {
        return OVERWRITE.equals(writeMode);
    }

    /** 水位线归属键：同一目标库下同表名也要区分数据源，避免不同 Doris 目标串水位线。 */
    public String targetKey() {
        return targetType + ":" + dataSourceId + ":" + tableName;
    }

    /** 目标表描述，用于节点日志/回写。 */
    public String targetLabel() {
        return targetLabelOverride != null
                ? targetLabelOverride : targetType + ":" + tableName;
    }

    public String getTargetLabelOverride() {
        return targetLabelOverride;
    }

    public void setTargetLabelOverride(String targetLabelOverride) {
        this.targetLabelOverride = targetLabelOverride;
    }
}
