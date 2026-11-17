package com.datamodel.common;

/**
 * 节点失败环节（M3.6 §11.3），由错误信息归类，物化表结构不新增列。
 */
public enum FailStage {

    SOURCE_READ,
    TYPE_MAPPING,
    STREAM_LOAD,
    DORIS_EXEC,
    VALIDATE,
    SINK_WRITE,
    OTHER;

    public static String classify(String errMsg) {
        if (errMsg == null || errMsg.trim().isEmpty()) {
            return OTHER.name();
        }
        String m = errMsg.toLowerCase();
        if (m.contains("sink 写入") || m.contains("sink写入") || m.contains("批插")
                || m.contains("目标表") || m.contains("写入失败")
                || (m.contains("第") && m.contains("批"))) {
            return SINK_WRITE.name();
        }
        if (m.contains("stream load")) {
            return STREAM_LOAD.name();
        }
        if (m.contains("类型映射") || m.contains("type mapping")) {
            return TYPE_MAPPING.name();
        }
        if (m.contains("同步行数不一致") || m.contains("校验") || m.contains("对账")) {
            return VALIDATE.name();
        }
        if (m.contains("读取源表") || m.contains("统计源表") || m.contains("数据源不存在")
                || m.contains("源表不存在") || m.contains("源表没有")
                || (m.contains("源表") && m.contains("失败"))) {
            return SOURCE_READ.name();
        }
        if (m.contains("doris 执行失败") || m.contains("doris 查询失败")
                || m.contains("物化") || m.contains("insert overwrite")) {
            return DORIS_EXEC.name();
        }
        return OTHER.name();
    }
}
